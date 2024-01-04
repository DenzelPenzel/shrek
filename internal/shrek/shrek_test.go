package shrek

import (
	"encoding/json"
	"github.com/draculaas/shrek/internal/config"
	"github.com/draculaas/shrek/internal/core"
	"github.com/draculaas/shrek/internal/utils"
	"github.com/stretchr/testify/require"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func Test_Shrek(t *testing.T) {
	t.Run("test open shrek single node", func(t *testing.T) {
		ms := createMockStore()
		defer os.RemoveAll(ms.Path())

		err := ms.Open(true)
		require.NoError(t, err)

		ms.WaitForLeader(10 * time.Second)
		leaderAddr, _ := ms.LeaderAddr()
		require.Equal(t, string(leaderAddr), ms.Addr())

		id, err := ms.LeaderID()
		require.NoError(t, err)

		require.Equal(t, id, ms.raftID)
	})

	t.Run("test open shrek close single node", func(t *testing.T) {
		ms := createMockStore()
		defer os.RemoveAll(ms.Path())

		err := ms.Open(true)
		require.NoError(t, err)

		ms.WaitForLeader(10 * time.Second)
		err = ms.Shutdown()
		require.NoError(t, err)
	})

	t.Run("test single node in-memory execute query", func(t *testing.T) {
		ms := createMockStore()
		defer os.RemoveAll(ms.Path())

		err := ms.Open(true)
		defer ms.Shutdown()
		require.NoError(t, err)

		ms.WaitForLeader(10 * time.Second)

		queries := []string{
			`create table test (id integer not null primary key, name text)`,
			`insert into test(id, name) values (1, "vasya")`,
		}

		resp, err := ms.Execute(&ExecuteRequest{queries, false, false})
		require.NoError(t, err)
		require.Equal(t, resp, resp)

		raw, err := ms.Query(&QueryRequest{[]string{`select * from test`}, false, false, None})
		require.NoError(t, err)

		res, err := json.Marshal(raw)
		require.NoError(t, err)
		require.Equal(t, `[{"columns":["id","name"],"types":["integer","text"],"values":[[1,"vasya"]]}]`, string(res))
	})

	t.Run("test single node in-memory execute query", func(t *testing.T) {
		ms := createMockStore()
		defer os.RemoveAll(ms.Path())

		err := ms.Open(true)
		defer ms.Shutdown()
		require.NoError(t, err)
		//
		ms.WaitForLeader(10 * time.Second)

		queries := []string{
			`insert into test(id, name) values (1, "vasya")`,
		}

		resp, err := ms.Execute(&ExecuteRequest{queries, false, false})
		require.NoError(t, err)
		res, err := json.Marshal(resp)
		require.NoError(t, err)
		require.Equal(t, `[{"error":"no such table: test"}]`, string(res))
	})

	t.Run("test single node multi execution query", func(t *testing.T) {
		ms := createMockStore()
		defer os.RemoveAll(ms.Path())

		err := ms.Open(true)
		defer ms.Shutdown()
		require.NoError(t, err)

		ms.WaitForLeader(10 * time.Second)

		queries := []string{
			`create table test (id integer not null primary key, name TEXT)`,
			`insert into test(id, name) values (1, "vasya")`,
		}

		_, err = ms.Execute(&ExecuteRequest{queries, false, false})
		require.NoError(t, err)

		for i := 0; i < 3; i++ {
			resp, err := ms.Query(&QueryRequest{
				[]string{`select * from test`},
				false,
				false,
				None,
			})
			require.NoError(t, err)
			res, err := json.Marshal(resp)
			require.NoError(t, err)
			require.Equal(t, `[{"columns":["id","name"],"types":["integer","text"],"values":[[1,"vasya"]]}]`, string(res))
		}
	})

	t.Run("test single node execution query tx", func(t *testing.T) {
		ms := createMockStore()
		defer os.RemoveAll(ms.Path())

		err := ms.Open(true)
		defer ms.Shutdown()
		require.NoError(t, err)

		ms.WaitForLeader(10 * time.Second)

		queries := []string{
			`create table test (id integer not null primary key, name TEXT)`,
			`insert into test(id, name) values (1, "vasya")`,
		}

		_, err = ms.Execute(&ExecuteRequest{queries, true, false})
		require.NoError(t, err)

		for i := 0; i < 3; i++ {
			resp, err := ms.Query(&QueryRequest{
				[]string{`select * from test`},
				false,
				true,
				None,
			})
			require.NoError(t, err)
			res, err := json.Marshal(resp)
			require.NoError(t, err)
			require.Equal(t, `[{"columns":["id","name"],"types":["integer","text"],"values":[[1,"vasya"]]}]`, string(res))
		}
	})

	t.Run("test single node load", func(t *testing.T) {
		ms := createMockStore()
		defer os.RemoveAll(ms.Path())

		err := ms.Open(true)
		defer ms.Shutdown()
		require.NoError(t, err)

		ms.WaitForLeader(10 * time.Second)

		dump := `
			pragma foreign_keys=off;
			begin transaction;
			create table test(id integer not null primary key, name text);
			insert into "test" values(1,'vasya');
			commit;
		`

		queries := []string{dump}

		_, err = ms.Execute(&ExecuteRequest{queries, false, false})
		require.NoError(t, err)

		resp, err := ms.Query(&QueryRequest{
			[]string{`select * from test`},
			false,
			true,
			Strong,
		})
		require.NoError(t, err)
		res, err := json.Marshal(resp)
		require.NoError(t, err)
		require.Equal(t, `[{"columns":["id","name"],"types":["integer","text"],"values":[[1,"vasya"]]}]`, string(res))
	})
}

type mockListener struct {
	root net.Listener
}

func (m *mockListener) Dial(addr string, timeout time.Duration) (net.Conn, error) {
	return net.DialTimeout("tcp", addr, timeout)
}

func (m *mockListener) Accept() (net.Conn, error) {
	return m.root.Accept()
}

func (m *mockListener) Close() error {
	return m.root.Close()
}

func (m *mockListener) Addr() net.Addr {
	return m.root.Addr()
}

func createMockLister(addr string) Listener {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		panic("failed to create the listener")
	}
	return &mockListener{
		root: ln,
	}
}

func createMockStore() *Shrek {
	dir := utils.TempDir("shrek-unit-test-")
	defer os.RemoveAll(dir)

	ln := createMockLister("localhost:0")

	httpAddr, _ := utils.GetTcpAddr("localhost:4001")
	raftAddr, _ := utils.GetTcpAddr("localhost:4002")
	raftHeartbeatTimeout, _ := time.ParseDuration("1s")
	raftElectionTimeout, _ := time.ParseDuration("1s")
	raftOpenTimeout, _ := time.ParseDuration("120s")
	raftApplyTimeout, _ := time.ParseDuration("10s")

	raftID := utils.RandomString(5)

	cfg := &config.Config{
		Environment: core.Local,
		ServerConfig: &config.ServerConfig{
			HttpAddr: httpAddr,
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

	return New(nil, cfg, ln)
}
