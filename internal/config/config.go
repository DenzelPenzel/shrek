package config

import (
	"github.com/draculaas/shrek/internal/core"
	"github.com/draculaas/shrek/internal/utils"
	"github.com/urfave/cli"
	"net"
	"path/filepath"
	"strings"
	"time"
)

// Config ... Application level configuration defined by `FilePath` value
type Config struct {
	Environment   core.Env
	ServerConfig  *ServerConfig
	CPUProfile    string
	StorageConfig *StorageConfig
}

// ServerConfig ... Server configuration options
type ServerConfig struct {
	HTTPAddr      net.Addr
	HTTPAdvertise *net.TCPAddr

	JoinServerHost string
	JoinServerPort int

	AllowedOrigins []string
	AllowedMethods []string
	AllowedHeaders []string
}

type StorageConfig struct {
	// DB config
	DBCfg *DBConfig

	// raft working dir
	RaftDir string

	// Node ID
	RaftID string

	// RaftAddr is the RPC address used by Nomad. This should be reachable
	// by the other servers and clients
	RaftAddr *net.TCPAddr

	// RaftAdvertise is the address that is advertised to client nodes for
	// the RPC endpoint. This can differ from the RPC address, if for example
	// the RaftAddr is unspecified "0.0.0.0:4646", but this address must be
	// reachable
	RaftAdvertise *net.TCPAddr

	RaftHeartbeatTimeout time.Duration

	// Join list of address
	Join string

	RaftElectionTimeout  time.Duration
	RaftApplyTimeout     time.Duration
	RaftOpenTimeout      time.Duration
	RaftSnapThreshold    uint64
	RaftShutdownOnRemove bool
}

type DBConfig struct {
	DBFilename string
	DSN        string // data source naming
	InMemory   bool   // in-memory mode
}

// NewConfig ... Initializer
func NewConfig(c *cli.Context) *Config {
	dbFilename := c.String("db-filename")
	dsn := c.String("dsn")
	inMemory := c.Bool("in-memory")

	env := c.String("env")
	raftNodeID := c.String("node-id")
	if raftNodeID == "" {
		raftNodeID = utils.RandomString(5)
	}

	httpAddr, _ := utils.GetTCPAddr(c.String("server-addr"))
	httpAdvertise, _ := utils.GetTCPAddr(c.String("server-advertise"))

	allowedOrigins := strings.Split(c.String("allowed-origins"), ",")
	allowedMethods := strings.Split(c.String("allowed-methods"), ",")
	allowedHeaders := strings.Split(c.String("allowed-headers"), ",")

	cpuProfile := c.String("cpu_profile")

	raftDir := c.String("raft-dir")
	if raftDir == "" {
		raftDir = utils.RandomString(5)
	}
	raftAddr, _ := utils.GetTCPAddr(c.String("raft-addr"))
	raftAdvertise, _ := utils.GetTCPAddr(c.String("raft-advertise"))

	raftHeartbeatTimeout, _ := time.ParseDuration(c.String("raft-heartbeat-timeout"))
	raftElectionTimeout, _ := time.ParseDuration(c.String("raft-election-timeout"))
	raftApplyTimeout, _ := time.ParseDuration(c.String("raft-apply-timeout"))
	raftOpenTimeout, _ := time.ParseDuration(c.String("raft-open-timeout"))
	raftSnapThreshold := c.Uint64("raft-snap-threshold")
	raftShutdownOnRemove := c.Bool("raft-shutdown-on-remove")

	join := c.String("join")

	config := &Config{
		Environment: core.Env(env),
		CPUProfile:  cpuProfile,

		ServerConfig: &ServerConfig{
			HTTPAddr:       httpAddr,
			HTTPAdvertise:  httpAdvertise,
			AllowedOrigins: allowedOrigins,
			AllowedMethods: allowedMethods,
			AllowedHeaders: allowedHeaders,
		},

		StorageConfig: &StorageConfig{
			DBCfg: &DBConfig{
				DBFilename: filepath.Join(raftDir, dbFilename),
				DSN:        dsn,
				InMemory:   inMemory,
			},
			RaftID:               raftNodeID,
			RaftDir:              raftDir,
			RaftAddr:             raftAddr,
			RaftAdvertise:        raftAdvertise,
			RaftHeartbeatTimeout: raftHeartbeatTimeout,
			RaftElectionTimeout:  raftElectionTimeout,
			RaftApplyTimeout:     raftApplyTimeout,
			RaftOpenTimeout:      raftOpenTimeout,
			RaftSnapThreshold:    raftSnapThreshold,
			RaftShutdownOnRemove: raftShutdownOnRemove,
			Join:                 join,
		},
	}

	return config
}

// IsProduction ... Returns true if the env is production
func (cfg *Config) IsProduction() bool {
	return cfg.Environment == core.Production
}

// IsDevelopment ... Returns true if the env is development
func (cfg *Config) IsDevelopment() bool {
	return cfg.Environment == core.Development
}

// IsLocal ... Returns true if the env is local
func (cfg *Config) IsLocal() bool {
	return cfg.Environment == core.Local
}
