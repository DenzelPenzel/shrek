package server

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/draculaas/shrek/internal/config"
	"github.com/draculaas/shrek/internal/core"
	sql "github.com/draculaas/shrek/internal/db"
	"github.com/draculaas/shrek/internal/shrek"
	"github.com/draculaas/shrek/internal/utils"
	"github.com/stretchr/testify/require"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func Test_HTTPServer(t *testing.T) {
	t.Run("test create a new server server", func(t *testing.T) {
		m := &MockStorage{}
		cfg := createMockConfig()
		s := New(context.TODO(), cfg.ServerConfig, m)
		defer s.ShutDown()
		err := s.Run()
		require.NoError(t, err)
	})

	t.Run("test content type", func(t *testing.T) {
		m := &MockStorage{}
		cfg := createMockConfig()
		s := New(context.TODO(), cfg.ServerConfig, m)
		defer s.ShutDown()
		err := s.Run()
		require.NoError(t, err)

		apiRequest := fmt.Sprintf("http://%s/api/db/stats", s.Addr().String())
		client := &http.Client{}

		resp, err := client.Get(apiRequest)
		require.NoError(t, err)
		require.Equal(t, "application/json", resp.Header.Get("Content-Type"))
		require.Equal(t, http.StatusOK, resp.StatusCode)
	})

	t.Run("test 404 route", func(t *testing.T) {
		m := &MockStorage{}
		cfg := createMockConfig()
		s := New(context.TODO(), cfg.ServerConfig, m)
		defer s.ShutDown()
		err := s.Run()
		require.NoError(t, err)

		apiRequest := fmt.Sprintf("http://%s/api/db", s.Addr().String())
		client := &http.Client{}

		resp, err := client.Get(apiRequest + "/test")
		require.NoError(t, err)
		require.Equal(t, http.StatusNotFound, resp.StatusCode)

		resp, err = client.Post(apiRequest+"/test", "", nil)
		require.NoError(t, err)
		require.Equal(t, http.StatusNotFound, resp.StatusCode)
	})

	t.Run("test execute abd query requests", func(t *testing.T) {
		m := &MockStorage{}
		cfg := createMockConfig()
		s := New(context.TODO(), cfg.ServerConfig, m)
		defer s.ShutDown()
		err := s.Run()
		require.NoError(t, err)

		host := fmt.Sprintf("http://%s/api/db", s.Addr().String())
		client := &http.Client{}

		resp, err := client.Get(host + "/execute")
		require.NoError(t, err)
		require.Equal(t, http.StatusNotFound, resp.StatusCode)

		testCases := []struct {
			api      string
			body     QueryRequest
			expected string
		}{
			{
				api: "/execute",
				body: QueryRequest{
					Queries:        []string{`create table test (id integer not null primary key, name text)`},
					UseTx:          false,
					IncludeTimings: false,
				},
				expected: `{"results":[{"last_insert_id":1,"time":3}]}`,
			},

			{
				api: "/query",
				body: QueryRequest{
					Queries:        []string{`select * from test`},
					UseTx:          false,
					IncludeTimings: false,
					Lvl:            shrek.Weak,
				},
				expected: `{"results":[{"columns":["id","name"],"types":["integer","text"],"values":[[1,"vasya"]]}]}`,
			},
		}

		for _, tc := range testCases {
			jsonData, err := json.Marshal(tc.body)
			require.NoError(t, err)
			reader := bytes.NewReader(jsonData)
			resp, err = client.Post(host+tc.api, "application/json", reader)
			require.NoError(t, err)
			defer resp.Body.Close()

			require.Equal(t, http.StatusOK, resp.StatusCode)

			body, err := io.ReadAll(resp.Body)
			require.NoError(t, err)
			require.Equal(t, tc.expected, string(body))
		}
	})

	t.Run("test set join node", func(t *testing.T) {
		mockSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method != http.MethodPost {
				t.Fatalf("invalid method name: %s", r.Method)
			}
			w.WriteHeader(http.StatusOK)
		}))
		defer mockSrv.Close()

		addr, _ := net.ResolveTCPAddr("tcp", "127.0.0.1:9090")
		resp, err := Join([]string{mockSrv.URL}, "node1", addr, nil)
		require.NoError(t, err)
		require.Equal(t, resp, mockSrv.URL+"/api/db/join")
	})

	t.Run("test set join node and parse meta", func(t *testing.T) {
		var body map[string]interface{}
		mockSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method != http.MethodPost {
				t.Fatalf("invalid method name: %s", r.Method)
			}
			w.WriteHeader(http.StatusOK)

			b, err := io.ReadAll(r.Body)
			if err != nil {
				w.WriteHeader(http.StatusBadRequest)
				return
			}

			if err := json.Unmarshal(b, &body); err != nil {
				w.WriteHeader(http.StatusBadRequest)
				return
			}
		}))
		defer mockSrv.Close()

		addr, _ := net.ResolveTCPAddr("tcp", "127.0.0.1:9090")
		meta := map[string]string{"test": "test"}

		resp, err := Join([]string{mockSrv.URL}, "node1", addr, meta)
		require.NoError(t, err)
		require.Equal(t, mockSrv.URL+"/api/db/join", resp)

		val, ok := body["id"]
		require.True(t, ok)
		require.Equal(t, "node1", val)

		val, ok = body["addr"]
		require.True(t, ok)
		require.Equal(t, addr.String(), val)

		val, ok = body["meta"]
		require.True(t, ok)

		val1, _ := json.Marshal(val)
		val2, _ := json.Marshal(meta)
		require.Equal(t, string(val1), string(val2))
	})

	t.Run("test set join node failed", func(t *testing.T) {
		mockSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusBadRequest)
		}))
		defer mockSrv.Close()

		addr, _ := net.ResolveTCPAddr("tcp", "127.0.0.1:9090")
		_, err := Join([]string{mockSrv.URL}, "node1", addr, nil)
		require.Error(t, err)
	})

	t.Run("test multi set join first node", func(t *testing.T) {
		mockSrv1 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
		}))
		defer mockSrv1.Close()

		mockSrv2 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
		}))
		defer mockSrv2.Close()

		addr, _ := net.ResolveTCPAddr("tcp", "127.0.0.1:9090")
		resp, err := Join([]string{mockSrv1.URL, mockSrv2.URL}, "node1", addr, nil)
		require.NoError(t, err)
		require.Equal(t, mockSrv1.URL+"/api/db/join", resp)
	})

	t.Run("test multi set join second node", func(t *testing.T) {
		mockSrv1 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusBadRequest)
		}))
		defer mockSrv1.Close()

		mockSrv2 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
		}))
		defer mockSrv2.Close()

		addr, _ := net.ResolveTCPAddr("tcp", "127.0.0.1:9090")
		resp, err := Join([]string{mockSrv1.URL, mockSrv2.URL}, "node1", addr, nil)
		require.NoError(t, err)
		require.Equal(t, mockSrv2.URL+"/api/db/join", resp)
	})

	t.Run("test multi set join second node redirect", func(t *testing.T) {
		mockSrv1 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
		}))
		defer mockSrv1.Close()
		redirectAddr := fmt.Sprintf("%s%s", mockSrv1.URL, "/join")

		mockSrv2 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			http.Redirect(w, r, redirectAddr, http.StatusMovedPermanently)
		}))
		defer mockSrv2.Close()

		addr, _ := net.ResolveTCPAddr("tcp", "127.0.0.1:9090")
		resp, err := Join([]string{mockSrv2.URL}, "node1", addr, nil)
		require.NoError(t, err)
		require.Equal(t, redirectAddr, resp)
	})
}

type MockStorage struct {
}

func (s *MockStorage) LeaderID() (string, error) {
	return "", nil
}

func (s *MockStorage) Stats() (map[string]interface{}, error) {
	return nil, nil //nolint:nilnil //Ignore it
}

func (s *MockStorage) GetMetadata(_, _ string) string {
	return ""
}

func (s *MockStorage) Execute(r *shrek.ExecuteRequest) ([]*sql.Result, error) {
	if !strings.HasPrefix(r.Queries[0], "create") {
		return nil, errors.New("wrong create SQL")
	}
	resp := &sql.Result{
		LastInsertID: 1,
		RowsAffected: 0,
		Error:        "",
		Time:         float64(3),
	}
	return []*sql.Result{resp}, nil
}

func (s *MockStorage) Query(r *shrek.QueryRequest) ([]*sql.Rows, error) {
	if !strings.HasPrefix(r.Queries[0], "select") {
		return nil, errors.New("wrong select SQL")
	}

	resp := &sql.Rows{
		Columns: []string{"id", "name"},
		Types:   []string{"integer", "text"},
		Values:  [][]interface{}{[]interface{}{1, "vasya"}},
		Error:   "",
		Time:    0,
	}

	return []*sql.Rows{resp}, nil
}

func (s *MockStorage) Join(_, _ string, _ map[string]string) error {
	return nil
}

func (s *MockStorage) Remove(_ string) error {
	return nil
}

func (s *MockStorage) Leader() string {
	return ""
}

func tempDir() string {
	path, err := os.MkdirTemp("", "shrek-test-")
	if err != nil {
		panic("failed to create temp dir")
	}
	return path
}

func createMockConfig() *config.Config {
	dir := tempDir()
	defer os.RemoveAll(dir)

	httpAddr, _ := utils.GetTCPAddr("localhost:4001")
	raftAddr, _ := utils.GetTCPAddr("localhost:4002")
	raftHeartbeatTimeout, _ := time.ParseDuration("1s")
	raftElectionTimeout, _ := time.ParseDuration("1s")
	raftOpenTimeout, _ := time.ParseDuration("120s")
	raftApplyTimeout, _ := time.ParseDuration("10s")

	raftID := utils.RandomString(5)

	return &config.Config{
		Environment: core.Local,
		ServerConfig: &config.ServerConfig{
			HTTPAddr: httpAddr,
		},
		StorageConfig: &config.StorageConfig{
			RaftID:               raftID,
			RaftDir:              filepath.Join(dir, raftID),
			RaftAddr:             raftAddr,
			RaftHeartbeatTimeout: raftHeartbeatTimeout,
			RaftElectionTimeout:  raftElectionTimeout,
			RaftApplyTimeout:     raftApplyTimeout,
			RaftOpenTimeout:      raftOpenTimeout,
			RaftSnapThreshold:    uint64(8192),
			RaftShutdownOnRemove: false,
			DBCfg: &config.DBConfig{
				DBFilename: filepath.Join(dir, "db.sqlite"),
				InMemory:   false,
				DSN:        "",
			},
		},
	}
}
