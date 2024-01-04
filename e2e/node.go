package e2e

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/draculaas/shrek/internal/config"
	"github.com/draculaas/shrek/internal/core"
	"github.com/draculaas/shrek/internal/dyport"
	"github.com/draculaas/shrek/internal/network"
	"github.com/draculaas/shrek/internal/server"
	"github.com/draculaas/shrek/internal/shrek"
	"github.com/draculaas/shrek/internal/utils"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

type Node struct {
	APIAddr  string
	RaftAddr string
	ID       string
	Dir      string
	Store    *shrek.Shrek
	Service  *server.Server
}

func (n *Node) Shutdown() {
	n.Store.Shutdown()
	n.Service.ShutDown()
	os.RemoveAll(n.Dir)
}

func (n *Node) sameAs(o *Node) bool {
	return n.RaftAddr == o.RaftAddr
}

func (n *Node) WaitForLeader() (string, error) {
	return n.Store.WaitForLeader(10 * time.Second)
}

func (n *Node) Execute(body server.QueryRequest) (string, error) {
	j, err := json.Marshal(body)
	if err != nil {
		return "", err
	}

	u, err := n.getApiURL("execute")
	if err != nil {
		return "", err
	}

	return n.postRequest(u, string(j))
}

func (n *Node) Query(body server.QueryRequest) (string, error) {
	u, err := n.getApiURL("query")
	if err != nil {
		return "", err
	}
	j, err := json.Marshal(body)
	if err != nil {
		return "", err
	}
	return n.postRequest(u, string(j))
}

func (n *Node) Status() (string, error) {
	v, _ := url.Parse("http://" + n.APIAddr + "/api/db/stats")
	resp, err := http.Get(v.String())
	if err != nil {
		return "", err
	}
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("invalid Status code: %q", resp.StatusCode)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}
	return string(body), nil
}

func (n *Node) getApiURL(suffix string) (*url.URL, error) {
	host := fmt.Sprintf("http://%s", n.APIAddr)
	u, err := url.Parse(fmt.Sprintf("%s/api/db/%s", host, suffix))
	if err != nil {
		return nil, err
	}
	return u, nil
}

func (n *Node) postRequest(u *url.URL, j string) (string, error) {
	resp, err := http.Post(u.String(), "application/json", strings.NewReader(j))
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}
	return string(b), err
}

func (n *Node) Join(leader *Node) error {
	resp, err := joinRequest(leader.APIAddr, n.Store.ID(), n.RaftAddr)
	if err != nil {
		return err
	}
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("failed to Join to the raft Cluster, Leader returned: %s", resp.Status)
	}
	defer resp.Body.Close()
	return nil
}

func joinRequest(nodeAddr, raftId, raftAddr string) (*http.Response, error) {
	b, err := json.Marshal(map[string]interface{}{"id": raftId, "addr": raftAddr})
	if err != nil {
		return nil, err
	}
	u, err := url.Parse("http://" + nodeAddr + "/api/db/join")
	if err != nil {
		return nil, err
	}
	resp, err := http.Post(u.String(), "application-type/json", bytes.NewReader(b))
	if err != nil {
		return nil, err
	}
	return resp, nil
}

func CreateNewNode(enableSingle bool) *Node {
	ctx := context.Background()
	node := &Node{
		Dir: utils.TempDir("shrek-interation-test-"),
	}

	nt := network.NewNetwork()
	if err := nt.Open(""); err != nil {
		panic(err.Error())
	}

	ports, err := dyport.AllocatePorts(2)
	if err != nil {
		return nil
	}

	httpAddr, _ := utils.GetTcpAddr(fmt.Sprintf("localhost:%s", strconv.Itoa(ports[0])))
	raftAddr, _ := utils.GetTcpAddr(fmt.Sprintf("localhost:%s", strconv.Itoa(ports[1])))
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
			RaftDir:              filepath.Join(node.Dir, raftID),
			RaftAddr:             raftAddr,
			RaftHeartbeatTimeout: raftHeartbeatTimeout,
			RaftElectionTimeout:  raftElectionTimeout,
			RaftApplyTimeout:     raftApplyTimeout,
			RaftOpenTimeout:      raftOpenTimeout,
			RaftSnapThreshold:    uint64(8192),
			RaftShutdownOnRemove: false,
			DBCfg: &config.DBConfig{
				DBFilename: filepath.Join(node.Dir, "db.sqlite"),
				InMemory:   false,
				DSN:        "",
			},
		},
	}

	node.Store = shrek.New(ctx, cfg, nt)

	if err := node.Store.Open(enableSingle); err != nil {
		node.Shutdown()
		panic(fmt.Sprintf("failed to open shrek: %s", err.Error()))
	}
	// store info about RaftAddr and ID
	node.RaftAddr = node.Store.Addr()
	node.ID = node.Store.ID()

	// launch server service
	node.Service = server.New(ctx, cfg.ServerConfig, node.Store)
	node.Service.Expvar = true
	if err := node.Service.Run(); err != nil {
		node.Shutdown()
		panic(fmt.Sprintf("failed to start HTTP service: %s", err.Error()))
	}

	node.APIAddr = node.Service.Addr().String()

	return node
}

func CreateLeaderNode() *Node {
	node := CreateNewNode(true)
	if _, err := node.WaitForLeader(); err != nil {
		node.Shutdown()
		panic(fmt.Sprintf("failed to achieve consensus: %s", err.Error()))
	}
	return node
}
