package server

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/draculaas/shrek/internal/config"
	sql "github.com/draculaas/shrek/internal/db"
	"github.com/draculaas/shrek/internal/logging"
	"github.com/draculaas/shrek/internal/shrek"
	ginzap "github.com/gin-contrib/zap"
	"github.com/gin-gonic/contrib/cors"
	"github.com/gin-gonic/gin"
	"go.uber.org/zap"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"time"
)

const numAttempts = 3

type Response struct {
	Results interface{} `json:"results,omitempty"`
	Error   string      `json:"error,omitempty"`
	Time    float64     `json:"time,omitempty"`

	start time.Time
	end   time.Time
}

type ExecuteRequest struct {
	Queries        []string `json:"queries,omitempty"`
	UseTx          bool     `json:"useTx,omitempty"`
	IncludeTimings bool     `json:"includeTimings,omitempty"`
}

type QueryRequest struct {
	Queries        []string               `json:"queries,omitempty"`
	UseTx          bool                   `json:"useTx,omitempty"`
	IncludeTimings bool                   `json:"includeTimings,omitempty"`
	Lvl            shrek.ConsistencyLevel `json:"lvl,omitempty"`
}

func (r *Response) SetTime() {
	r.Time = r.end.Sub(r.start).Seconds()
}

func NewResponse() *Response {
	return &Response{
		start: time.Now(),
	}
}

type Server struct {
	ln net.Listener

	addr    net.Addr
	cfg     *config.ServerConfig
	storage Storage
	srv     *http.Server

	start time.Time

	statuses map[string]StatsI

	Expvar bool
	Pprof  bool

	BuildInfo map[string]interface{}

	logger *zap.Logger
}

// StatsI ...  Interface that status providers are required to implement
type StatsI interface {
	Stats() (interface{}, error)
}

type Storage interface {
	Execute(r *shrek.ExecuteRequest) ([]*sql.Result, error)

	Query(r *shrek.QueryRequest) ([]*sql.Rows, error)

	// Join ... Connects the node,
	// identified by the provided ID and reachable at the specified address, to the current node
	Join(id, addr string, metadata map[string]string) error

	// Remove ... Remove node from the cluster
	Remove(addr string) error

	// LeaderID ... Returns the raft leader
	LeaderID() (string, error)

	// Stats ... Returns info about the storage
	Stats() (map[string]interface{}, error)

	// GetMetadata ... Returns metadata for specific node_id and for a given key
	GetMetadata(id, key string) string
}

func New(ctx context.Context, cfg *config.ServerConfig, s Storage) *Server {
	logger := logging.WithContext(ctx)
	return &Server{
		cfg:      cfg,
		addr:     cfg.HTTPAddr,
		storage:  s,
		start:    time.Now(),
		statuses: make(map[string]StatsI),
		logger:   logger,
	}
}

func (s *Server) Run() error {
	router := gin.Default()

	router.Use(ginzap.Ginzap(s.logger, time.RFC3339, true))
	router.Use(ginzap.RecoveryWithZap(s.logger, true))

	if s.cfg.AllowedOrigins != nil && s.cfg.AllowedMethods != nil {
		allowAllOrigins := len(s.cfg.AllowedOrigins) == 1 && s.cfg.AllowedOrigins[0] == "*"
		allowedOrigins := s.cfg.AllowedOrigins
		if allowAllOrigins {
			allowedOrigins = nil
		}
		router.Use(cors.New(cors.Config{
			AllowAllOrigins: allowAllOrigins,
			AllowedOrigins:  allowedOrigins,
			AllowedMethods:  s.cfg.AllowedMethods,
			AllowedHeaders:  s.cfg.AllowedHeaders,
		}))
	}

	router.POST("/api/db/execute", s.handleExecute())
	router.POST("/api/db/query", s.handleQuery())
	router.GET("/api/db/stats", s.handleStats())
	router.DELETE("/api/db/remove", s.handleRemove())
	router.POST("/api/db/join", s.handleJoin())

	srv := &http.Server{
		Handler:           router,
		ReadHeaderTimeout: 5 * time.Second,
	}

	ln, err := net.Listen("tcp", s.addr.String())
	if err != nil {
		return err
	}

	s.ln = ln
	s.srv = srv

	go func() {
		err := srv.Serve(s.ln)
		if err != nil {
			s.logger.Info("failed to execute HTTP Server() call", zap.Error(err))
		}
	}()

	s.logger.Info("service running on", zap.String("addr", s.addr.String()))
	s.srv = srv

	return nil
}

func (s *Server) ShutDown() {
	s.ln.Close()
	s.srv.Close()
}

func (s *Server) Addr() net.Addr {
	return s.ln.Addr()
}

// Join ... Sequentially processes joinAddr, updating node ID (id) and Raft
// address (addr) for each joining node. It returns the successfully used endpoint
// for joining the cluster.
//
// The function walks through joinAddr, associating each node with its ID and Raft
// address. The resulting endpoint is the successfully utilized address during the
// cluster joining process.
//
// Example:
//
//	endpoint, err := Join(joinAddr, id, addr, meta)
//	if err != nil {
//	    log.Fatal("Failed to set joining node information:", err)
//	}
//	log.Printf("Successfully joined the cluster. Endpoint: %s", endpoint)
func Join(jA []string, id string, addr *net.TCPAddr, meta map[string]string) (string, error) {
	var err error
	var fullURL string
	logger := logging.NoContext()
	joinAddr := make([]*url.URL, 0)
	for _, a := range jA {
		u, err := url.Parse(fmt.Sprintf("%s/api/db/join", a))
		if err != nil {
			return "", err
		}
		joinAddr = append(joinAddr, u)
	}

	for i := 0; i < numAttempts; i++ {
		for _, joinAddr := range joinAddr {
			fullURL, err = join(joinAddr, id, addr, meta)
			if err == nil {
				return fullURL, nil
			}
		}
		time.Sleep(2 * time.Second)
	}

	logger.Error("failed to join raft cluster",
		zap.String("attempts", strconv.Itoa(numAttempts)),
		zap.Error(err),
	)

	return "", err
}

func join(joinAddr *url.URL, id string, addr *net.TCPAddr, meta map[string]string) (string, error) {
	logger := logging.NoContext()
	if id == "" {
		return "", fmt.Errorf("node ID is empty")
	}

	tr := &http.Transport{}
	client := &http.Client{Transport: tr}
	client.CheckRedirect = func(req *http.Request, via []*http.Request) error {
		return http.ErrUseLastResponse
	}

	for {
		b, err := json.Marshal(map[string]interface{}{
			"id":   id,
			"addr": addr.String(),
			"meta": meta,
		})
		if err != nil {
			return "", err
		}

		resp, err := client.Post( //nolint:noctx // no need ctx
			joinAddr.String(),
			"application-type/json",
			bytes.NewReader(b),
		)
		if err != nil {
			return "", err
		}
		defer resp.Body.Close()

		logger.Debug("Join request, method: POST", zap.String("url", joinAddr.String()))

		b, err = io.ReadAll(resp.Body)
		if err != nil {
			return "", err
		}

		switch resp.StatusCode {
		case http.StatusOK:
			return joinAddr.String(), nil
		case http.StatusMovedPermanently:
			redirectURL := resp.Header.Get("location")
			if redirectURL == "" {
				return "", fmt.Errorf("failed to join, invalid redirect received")
			}
			joinAddr, err = url.Parse(redirectURL)
			if err != nil {
				return "", fmt.Errorf("failed to join, invalid redirect received")
			}
			continue
		case http.StatusBadRequest:
			if joinAddr.Scheme == "https" {
				return "", fmt.Errorf("failed to join, node returned: %s: (%s)", resp.Status, string(b))
			}
			logger.Info("join via HTTP failed, trying via HTTPS")
			joinAddr.Scheme = "https"
			continue
		default:
			return "", fmt.Errorf("failed to join, node returned: %s: (%s)", resp.Status, string(b))
		}
	}
}

func (s *Server) leaderAPIAddr() string {
	id, err := s.storage.LeaderID()
	if err != nil {
		return ""
	}
	return s.storage.GetMetadata(id, "api_addr")
}

func (s *Server) leaderID() string {
	id, err := s.storage.LeaderID()
	if err != nil {
		return ""
	}
	return id
}
