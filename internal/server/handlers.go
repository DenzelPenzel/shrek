package server

import (
	"bytes"
	"errors"
	"fmt"
	"github.com/draculaas/shrek/internal/shrek"
	"github.com/gin-gonic/gin"
	"net/http"
	"runtime"
	"time"
)

func (s *Server) handleExecute() gin.HandlerFunc {
	return func(c *gin.Context) {
		c.Header("Content-Type", "application/json; charset=utf-8")

		response := NewResponse()

		var req ExecuteRequest
		if err := c.ShouldBindJSON(&req); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{
				"error": fmt.Sprintf("Bad request: %v", err),
			})
		}

		res, err := s.storage.Execute(&shrek.ExecuteRequest{
			Queries:        req.Queries,
			UseTx:          req.UseTx,
			IncludeTimings: req.IncludeTimings,
		})
		if err != nil {
			if errors.Is(err, shrek.ErrNotLeader) {
				leader := s.leaderID()
				if leader == "" {
					c.JSON(http.StatusServiceUnavailable, gin.H{
						"error": err.Error(),
					})
					return
				}
				// redirect to the leader
			}
			response.Error = err.Error()
		} else {
			response.Results = res
		}
		// update end request time
		response.end = time.Now()
		c.JSON(http.StatusOK, response)
	}
}

func (s *Server) handleQuery() gin.HandlerFunc {
	return func(c *gin.Context) {
		c.Header("Content-Type", "application/json; charset=utf-8")

		var req QueryRequest
		if err := c.ShouldBindJSON(&req); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{
				"error": fmt.Sprintf("Bad request: %v", err),
			})
		}

		resp := NewResponse()

		res, err := s.storage.Query(&shrek.QueryRequest{
			Queries:        req.Queries,
			UseTx:          req.UseTx,
			IncludeTimings: req.IncludeTimings,
			Lvl:            req.Lvl,
		})
		if err != nil {
			if errors.Is(err, shrek.ErrNotLeader) {
				leader := s.leaderID()
				if leader == "" {
					c.JSON(http.StatusServiceUnavailable, gin.H{
						"error": err.Error(),
					})
					return
				}
				// TODO
			}
			resp.Error = err.Error()
		} else {
			resp.Results = res
		}

		resp.end = time.Now()
		c.JSON(http.StatusOK, resp)
	}
}

func (s *Server) handleStats() gin.HandlerFunc {
	return func(c *gin.Context) {
		c.Header("Content-Type", "application/json")

		storageStats, err := s.storage.Stats()
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{
				"error": err,
			})
			return
		}

		stats := map[string]interface{}{
			"runtime": map[string]interface{}{
				"GOARCH":        runtime.GOARCH,
				"GOOS":          runtime.GOOS,
				"GOMAXPROCS":    runtime.GOMAXPROCS(0),
				"num_cpu":       runtime.NumCPU(),
				"num_goroutine": runtime.NumGoroutine(),
				"version":       runtime.Version(),
			},
			"storage": storageStats,
			"server": map[string]interface{}{
				"addr": s.addr.String(),
			},
			"node": map[string]interface{}{
				"start_time": s.start,
				"uptime":     time.Since(s.start).String(),
			},
		}

		c.JSON(http.StatusOK, stats)
	}
}

func (s *Server) handleRemove() gin.HandlerFunc {
	return func(c *gin.Context) {

	}
}

func (s *Server) handleJoin() gin.HandlerFunc {
	return func(c *gin.Context) {
		var req map[string]interface{}
		if err := c.ShouldBindJSON(&req); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{
				"error": fmt.Sprintf("Bad request: %v", err),
			})
		}
		id, ok := req["id"]
		if !ok {
			c.JSON(http.StatusBadRequest, gin.H{
				"error": errors.New("missing id param"),
			})
			return
		}

		var meta map[string]string
		if _, ok := req["meta"].(map[string]interface{}); ok {
			meta = make(map[string]string)
			for key, value := range req["meta"].(map[string]interface{}) {
				if stringValue, ok := value.(string); ok {
					meta[key] = stringValue
				}
			}
		}

		addr, ok := req["addr"]
		if !ok {
			c.JSON(http.StatusBadRequest, gin.H{
				"error": errors.New("missing addr param"),
			})
			return
		}

		if err := s.storage.Join(id.(string), addr.(string), meta); err != nil {
			if errors.Is(err, shrek.ErrNotLeader) {
				leader := s.leaderAPIAddr()
				if leader == "" {
					c.JSON(http.StatusServiceUnavailable, gin.H{
						"error": err.Error(),
					})
					return
				}
			}
			b := bytes.NewBufferString(err.Error())
			c.JSON(http.StatusInternalServerError, b)
		}
	}
}
