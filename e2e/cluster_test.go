package e2e

import (
	"encoding/json"
	"github.com/draculaas/shrek/internal/server"
	"github.com/draculaas/shrek/internal/shrek"
	"github.com/stretchr/testify/require"
	"testing"
)

func Test_SingleNodeCRUD(t *testing.T) {
	node := CreateLeaderNode()
	defer node.Shutdown()

	testCases := []struct {
		body     server.QueryRequest
		expected string
		execute  bool
	}{
		{
			body: server.QueryRequest{
				Queries:        []string{`create table test(id integer not null primary key, name text)`},
				UseTx:          false,
				IncludeTimings: false,
			},
			expected: `{"results":[{}]}`,
			execute:  true,
		},
		{
			body: server.QueryRequest{
				Queries:        []string{`insert into test(name) values ("vasya")`},
				UseTx:          false,
				IncludeTimings: false,
			},
			expected: `{"results":[{"last_insert_id":1,"rows_affected":1}]}`,
			execute:  true,
		},
		{
			body: server.QueryRequest{
				Queries:        []string{`insert into test2(name) values("ivan")`},
				UseTx:          false,
				IncludeTimings: false,
			},
			expected: `{"results":[{"error":"no such table: test2"}]}`,
			execute:  true,
		},
		{
			body: server.QueryRequest{
				Queries:        []string{`insert ahah ahah`},
				UseTx:          false,
				IncludeTimings: false,
			},
			expected: `{"results":[{"error":"near \"ahah\": syntax error"}]}`,
			execute:  true,
		},
		{
			body: server.QueryRequest{
				Queries:        []string{`select * from test`},
				UseTx:          false,
				IncludeTimings: false,
				Lvl:            shrek.Strong,
			},
			expected: `{"results":[{"columns":["id","name"],"types":["integer","text"],"values":[[1,"vasya"]]}]}`,
			execute:  false,
		},
		{
			body: server.QueryRequest{
				Queries:        []string{`drop table test`},
				UseTx:          false,
				IncludeTimings: false,
			},
			expected: `{"results":[{"last_insert_id":1,"rows_affected":1}]}`,
			execute:  true,
		},
		{
			body: server.QueryRequest{
				Queries:        []string{`drop table test`},
				UseTx:          false,
				IncludeTimings: false,
			},
			expected: `{"results":[{"error":"no such table: test"}]}`,
			execute:  true,
		},
		// TODO double check this test cases
		{
			body: server.QueryRequest{
				Queries:        []string{`create table aaa (id integer not null primary key, name text)`},
				UseTx:          false,
				IncludeTimings: false,
			},
			expected: `{"results":[{"last_insert_id":1,"rows_affected":1}]}`,
			execute:  true,
		},
		{
			body: server.QueryRequest{
				Queries:        []string{`create table bbb (id integer not null primary key, sequence integer)`},
				UseTx:          false,
				IncludeTimings: false,
			},
			expected: `{"results":[{"last_insert_id":1,"rows_affected":1}]}`,
			execute:  true,
		},
		{
			body: server.QueryRequest{
				Queries:        []string{`insert into aaa(name) values("ana")`},
				UseTx:          false,
				IncludeTimings: false,
			},
			expected: `{"results":[{"last_insert_id":1,"rows_affected":1}]}`,
			execute:  true,
		},
		{
			body: server.QueryRequest{
				Queries:        []string{`insert into aaa(name) values("denis")`},
				UseTx:          false,
				IncludeTimings: false,
			},
			expected: `{"results":[{"last_insert_id":2,"rows_affected":1}]}`,
			execute:  true,
		},
		{
			body: server.QueryRequest{
				Queries:        []string{`insert into bbb(sequence) values(10)`},
				UseTx:          false,
				IncludeTimings: false,
			},
			expected: `{"results":[{"last_insert_id":1,"rows_affected":1}]}`,
			execute:  true,
		},
	}

	for _, tc := range testCases {
		var resp string
		var err error
		if tc.execute {
			resp, err = node.Execute(tc.body)
		} else {
			resp, err = node.Query(tc.body)
		}
		require.NoError(t, err)
		require.Equal(t, tc.expected, resp)
	}
}

func Test_SingleNodeMutilInsert(t *testing.T) {
	node := CreateLeaderNode()
	defer node.Shutdown()

	testCases := []struct {
		body     server.QueryRequest
		expected string
		execute  bool
	}{
		{
			body: server.QueryRequest{
				Queries:        []string{`create table aaa (id integer not null primary key, name text)`},
				UseTx:          false,
				IncludeTimings: false,
			},
			expected: `{"results":[{}]}`,
			execute:  true,
		},
		{
			body: server.QueryRequest{
				Queries:        []string{`create table bbb (id integer not null primary key, sequence integer)`},
				UseTx:          false,
				IncludeTimings: false,
			},
			expected: `{"results":[{}]}`,
			execute:  true,
		},
		{
			body: server.QueryRequest{
				Queries:        []string{`insert into aaa(name) values("ana")`},
				UseTx:          false,
				IncludeTimings: false,
			},
			expected: `{"results":[{"last_insert_id":1,"rows_affected":1}]}`,
			execute:  true,
		},
		{
			body: server.QueryRequest{
				Queries:        []string{`insert into aaa(name) values("denis")`},
				UseTx:          false,
				IncludeTimings: false,
			},
			expected: `{"results":[{"last_insert_id":2,"rows_affected":1}]}`,
			execute:  true,
		},
		{
			body: server.QueryRequest{
				Queries:        []string{`insert into bbb(sequence) values(10)`},
				UseTx:          false,
				IncludeTimings: false,
			},
			expected: `{"results":[{"last_insert_id":1,"rows_affected":1}]}`,
			execute:  true,
		},
		{
			body: server.QueryRequest{
				Queries:        []string{`select * from aaa`, `select * from bbb`},
				UseTx:          false,
				IncludeTimings: false,
			},
			expected: `{"results":[{"columns":["id","name"],"types":["integer","text"],"values":[[1,"ana"],[2,"denis"]]},{"columns":["id","sequence"],"types":["integer","integer"],"values":[[1,10]]}]}`,
			execute:  false,
		},
	}

	for _, tc := range testCases {
		var resp string
		var err error
		if tc.execute {
			resp, err = node.Execute(tc.body)
		} else {
			resp, err = node.Query(tc.body)
		}
		require.NoError(t, err)
		require.Equal(t, tc.expected, resp)
	}

}

func Test_APIStatus(t *testing.T) {
	node := CreateLeaderNode()
	defer node.Shutdown()

	resp, err := node.Status()
	require.NoError(t, err)

	var j map[string]interface{}
	err = json.Unmarshal([]byte(resp), &j)
	require.NoError(t, err)
}

func Test_MultiNodes(t *testing.T) {
	node1 := CreateLeaderNode()
	defer node1.Shutdown()

	node2 := CreateNewNode(false)
	defer node1.Shutdown()

	err := node2.Join(node1)
	require.NoError(t, err)

	lAddr, err := node2.WaitForLeader()
	require.NoError(t, err)
	require.Equal(t, node1.RaftAddr, lAddr)

	c := &Cluster{
		nodes: []*Node{node1, node2},
	}

	l, err := c.Leader()
	require.NoError(t, err)

	node3 := CreateNewNode(false)
	defer node3.Shutdown()
	err = node3.Join(l)
	require.NoError(t, err)

	lAddr, err = node3.WaitForLeader()
	require.NoError(t, err)
	require.Equal(t, node1.RaftAddr, lAddr)

	c = &Cluster{
		nodes: []*Node{node1, node2, node3},
	}
	l, err = c.Leader()
	require.NoError(t, err)

	testCases := []struct {
		body     server.QueryRequest
		expected string
		execute  bool
	}{
		{
			body: server.QueryRequest{
				Queries:        []string{`create table test (id integer not null primary key, name text)`},
				UseTx:          false,
				IncludeTimings: false,
			},
			expected: `{"results":[{}]}`,
			execute:  true,
		},
		{
			body: server.QueryRequest{
				Queries:        []string{`insert into test(name) values("vasya")`},
				UseTx:          false,
				IncludeTimings: false,
			},
			expected: `{"results":[{"last_insert_id":1,"rows_affected":1}]}`,
			execute:  true,
		},
		{
			body: server.QueryRequest{
				Queries:        []string{`select * from test`},
				UseTx:          false,
				IncludeTimings: false,
			},
			expected: `{"results":[{"columns":["id","name"],"types":["integer","text"],"values":[[1,"vasya"]]}]}`,
			execute:  false,
		},
	}

	for _, tc := range testCases {
		var resp string
		var err error
		if tc.execute {
			resp, err = l.Execute(tc.body)
		} else {
			resp, err = l.Query(tc.body)
		}
		require.NoError(t, err)
		require.Equal(t, tc.expected, resp)
	}

	// kill the Leader qpwjl
	t.Logf("kill the Leader Node %s, waiting the new Leader...", l.ID)
	l.Shutdown()
	c.RemoveNode(l)

	l, err = c.WaitForNewLeader(l)
	require.NoError(t, err)

	t.Logf("elected the new Leader Node %s", l.ID)

	t.Log("now Cluster contains 2 nodes, running queries...")

	testCases = []struct {
		body     server.QueryRequest
		expected string
		execute  bool
	}{
		{
			body: server.QueryRequest{
				Queries:        []string{`create table test (id integer not null primary key, name text)`},
				UseTx:          false,
				IncludeTimings: false,
			},
			expected: `{"results":[{"error":"table test already exists"}]}`,
			execute:  true,
		},
		{
			body: server.QueryRequest{
				Queries:        []string{`insert into test(name) values("ana")`},
				UseTx:          false,
				IncludeTimings: false,
			},
			expected: `{"results":[{"last_insert_id":2,"rows_affected":1}]}`,
			execute:  true,
		},
		{
			body: server.QueryRequest{
				Queries:        []string{`select * from test`},
				UseTx:          false,
				IncludeTimings: false,
			},
			expected: `{"results":[{"columns":["id","name"],"types":["integer","text"],"values":[[1,"vasya"],[2,"ana"]]}]}`,
			execute:  false,
		},
	}

	for _, tc := range testCases {
		var resp string
		var err error
		if tc.execute {
			resp, err = l.Execute(tc.body)
		} else {
			resp, err = l.Query(tc.body)
		}
		require.NoError(t, err)
		require.Equal(t, tc.expected, resp)
	}

}
