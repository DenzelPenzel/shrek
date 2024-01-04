package shrek

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/draculaas/shrek/internal/config"
	sql "github.com/draculaas/shrek/internal/db"
	"github.com/draculaas/shrek/internal/logging"
	"github.com/draculaas/shrek/internal/utils"
	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb"
	"go.uber.org/zap"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"
)

type storagePayload struct {
	Queries        []string `json:"queries,omitempty"`
	UseTx          bool     `json:"useTx,omitempty"`
	IncludeTimings bool     `json:"includeTimings,omitempty"`
}

type payload struct {
	Typ payloadType     `json:"typ,omitempty"`
	Sub json.RawMessage `json:"sub,omitempty"`
}

type metadataPayload struct {
	RaftID string            `json:"raft_id,omitempty"`
	Data   map[string]string `json:"data,omitempty"`
}

var (
	// ErrNotLeader is returned when a node attempts to execute a leader-only
	// operation.
	ErrNotLeader = errors.New("not leader")

	// ErrOpenTimeout is returned when the Shrek does not apply its initial
	// logs within the specified time.
	ErrOpenTimeout = errors.New("timeout waiting for initial logs application")

	// ErrInvalidBackupFormat is returned when the requested backup format
	// is not valid.
	ErrInvalidBackupFormat = errors.New("invalid backup format")
)

const (
	retainSnapshotCount = 2
)

// Shrek - SQLite database, supporting Raft as consensus protocol
type Shrek struct {
	// raft
	raft *raft.Raft

	raftLayer     Listener
	raftTransport *raft.NetworkTransport

	// physical store
	boltStore *raftboltdb.BoltStore
	// kvs persistent store
	raftStable raft.StableStore

	// log storage
	raftLog raft.LogStore

	raftDir string
	raftID  string

	mu sync.RWMutex

	ctx context.Context

	// SQLite store
	db *sql.DB
	// SQLite database config
	dbConf *config.DBConfig

	meta   map[string]map[string]string
	metaMu sync.RWMutex

	ShutdownOnRemove  bool
	SnapshotThreshold uint64
	SnapshotInterval  time.Duration
	HeartbeatTimeout  time.Duration
	ElectionTimeout   time.Duration
	ApplyTimeout      time.Duration
	OpenTimeout       time.Duration

	logger *zap.Logger

	shutdown     bool
	shutdownLock sync.Mutex
}

// Apply ... Applies a Raft log entry to the database
func (s *Shrek) Apply(l *raft.Log) interface{} {
	var c payload
	if err := json.Unmarshal(l.Data, &c); err != nil {
		panic(fmt.Sprintf("failed to unmarshal raft command: %s", err.Error()))
	}

	switch c.Typ {
	case Execute, Query:
		var d storagePayload
		if err := json.Unmarshal(c.Sub, &d); err != nil {
			return &fsmGenericResponse{error: err}
		}
		if c.Typ == Execute {
			r, err := s.db.Execute(d.Queries, d.UseTx, d.IncludeTimings)
			return &fsmExecuteResponse{results: r, error: err}
		}
		r, err := s.db.Query(d.Queries, d.UseTx, d.IncludeTimings)
		return &fsmQueryResponse{rows: r, error: err}
	case Peer:
		var data metadataPayload
		if err := json.Unmarshal(c.Sub, &data); err != nil {
			return &fsmGenericResponse{error: err}
		}
		func() {
			s.metaMu.Lock()
			defer s.metaMu.Unlock()
			if _, ok := s.meta[data.RaftID]; !ok {
				s.meta[data.RaftID] = make(map[string]string)
			}
			for k, v := range data.Data {
				s.meta[data.RaftID][k] = v
			}
		}()
		return &fsmGenericResponse{}
	default:
		return &fsmGenericResponse{error: fmt.Errorf("unknown command: %v", c.Typ)}
	}
}

func (s *Shrek) Database(leader bool) ([]byte, error) {
	if leader && s.raft.State() != raft.Leader {
		return nil, errors.New("leader is missing")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	f, err := os.CreateTemp("", "shrek-snap-")
	if err != nil {
		return nil, err
	}
	f.Close()
	defer os.Remove(f.Name())

	if err := s.db.Backup(f.Name()); err != nil {
		return nil, err
	}

	return os.ReadFile(f.Name())
}

func (s *Shrek) Snapshot() (raft.FSMSnapshot, error) {
	fsm := &fsmSnapshot{}
	logger := logging.WithContext(s.ctx)
	var err error
	fsm.database, err = s.Database(false)
	if err != nil {
		logger.Error("error connecting to database", zap.Error(err))
		return nil, err
	}

	fsm.meta, err = json.Marshal(s.meta)
	if err != nil {
		logger.Error("error encode metadata for snapshot", zap.Error(err))
		return nil, err
	}

	return fsm, nil
}

// Restore ... Restores the node to a previous state
func (s *Shrek) Restore(snapshot io.ReadCloser) error {
	if err := s.db.Close(); err != nil {
		return err
	}

	// get size of the database
	var size uint64
	if err := binary.Read(snapshot, binary.LittleEndian, &size); err != nil {
		return err
	}

	// read in the database file data and restore
	database := make([]byte, size)
	if _, err := io.ReadFull(snapshot, database); err != nil {
		return err
	}

	var db *sql.DB
	var err error

	if err := os.WriteFile(s.dbConf.DBFilename, database, 0600); err != nil {
		return err
	}

	db, err = sql.OpenWithDSN(s.dbConf.DBFilename, s.dbConf.DSN)
	if err != nil {
		return err
	}

	s.db = db

	// Read remaining bytes, and set to cluster meta.
	b, err := io.ReadAll(snapshot)
	if err != nil {
		return err
	}

	s.metaMu.Lock()
	defer s.metaMu.Unlock()
	err = json.Unmarshal(b, &s.meta)
	if err != nil {
		return err
	}

	return nil
}

// GetMetadata ... Returns metadata for specific node_id and for a given key
func (s *Shrek) GetMetadata(id, key string) string {
	s.metaMu.RLock()
	defer s.metaMu.RUnlock()

	if _, ok := s.meta[id]; !ok {
		return ""
	}
	v, ok := s.meta[id][key]
	if !ok {
		return ""
	}
	return v
}

func (s *Shrek) SetMetadata(md map[string]string) error {
	return s.setMetadata(s.raftID, md)
}

func (s *Shrek) setMetadata(id string, md map[string]string) error {
	if func() bool {
		s.metaMu.RLock()
		defer s.metaMu.RUnlock()

		if _, ok := s.meta[id]; ok {
			for k, v := range md {
				if s.meta[id][k] != v {
					return false
				}
			}
			return true
		}
		return false
	}() {
		return nil
	}

	p, err := json.Marshal(metadataPayload{
		RaftID: id,
		Data:   md,
	})
	if err != nil {
		return err
	}

	b, err := json.Marshal(payload{Typ: Peer, Sub: p})
	if err != nil {
		return err
	}

	f := s.raft.Apply(b, s.ApplyTimeout)
	if e, ok := f.(raft.Future); ok && e.Error() != nil {
		if errors.Is(e.Error(), raft.ErrNotLeader) {
			return ErrNotLeader
		}
		err := e.Error()
		if err != nil {
			return err
		}
	}

	return nil
}

func (s *Shrek) Execute(r *ExecuteRequest) ([]*sql.Result, error) {
	if s.raft.State() != raft.Leader {
		return nil, ErrNotLeader
	}

	body, err := json.Marshal(&storagePayload{
		Queries:        r.Queries,
		UseTx:          r.UseTx,
		IncludeTimings: r.IncludeTimings,
	})
	if err != nil {
		return nil, err
	}

	b, err := json.Marshal(&payload{
		Typ: Execute,
		Sub: body,
	})
	if err != nil {
		return nil, err
	}

	f := s.raft.Apply(b, s.ApplyTimeout)
	if e, ok := f.(raft.Future); ok && e.Error() != nil {
		if errors.Is(e.Error(), raft.ErrNotLeader) {
			return nil, ErrNotLeader
		}
		return nil, e.Error()
	}

	res, ok := f.Response().(*fsmExecuteResponse)
	if !ok {
		return nil, errors.New("failed response type")
	}
	return res.results, res.error
}

func (s *Shrek) Query(r *QueryRequest) ([]*sql.Rows, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if r.Lvl == Strong { //nolint:nestif //todo
		body, err := json.Marshal(&storagePayload{
			UseTx:          r.UseTx,
			Queries:        r.Queries,
			IncludeTimings: r.IncludeTimings,
		})
		if err != nil {
			return nil, err
		}
		b, err := json.Marshal(&payload{
			Typ: Query,
			Sub: body,
		})
		if err != nil {
			return nil, err
		}
		f := s.raft.Apply(b, s.ApplyTimeout)
		if e, ok := f.(raft.Future); ok && e.Error() != nil {
			if errors.Is(e.Error(), raft.ErrNotLeader) {
				return nil, ErrNotLeader
			}
			return nil, e.Error()
		}

		res, ok := f.Response().(*fsmQueryResponse)
		if !ok {
			return nil, errors.New("invalid response")
		}
		return res.rows, res.error
	}

	if r.Lvl == Weak && s.raft.State() != raft.Leader {
		return nil, ErrNotLeader
	}

	return s.db.Query(r.Queries, r.UseTx, r.IncludeTimings)
}

func (s *Shrek) Join(id, addr string, metadata map[string]string) error {
	s.logger.Info("Received a request to join node", zap.String("addr", addr))

	if s.raft.State() != raft.Leader {
		return ErrNotLeader
	}

	f := s.raft.AddVoter(raft.ServerID(id), raft.ServerAddress(addr), 0, 0)

	if e, ok := f.(raft.Future); ok && e.Error() != nil {
		if errors.Is(e.Error(), raft.ErrNotLeader) {
			return ErrNotLeader
		}
		return e.Error()
	}

	if err := s.setMetadata(id, metadata); err != nil {
		return err
	}

	s.logger.Info("Successfully joined the node", zap.String("addr", addr))

	return nil
}

// Remove ... Removes a node from the store
func (s *Shrek) Remove(addr string) error {
	s.logger.Info("Received a request to join node", zap.String("addr", addr))

	if s.raft.State() != raft.Leader {
		return ErrNotLeader
	}

	f := s.raft.RemoveServer(raft.ServerID(addr), 0, 0)
	if f.Error() != nil {
		if errors.Is(f.Error(), raft.ErrNotLeader) {
			return ErrNotLeader
		}
		return f.Error()
	}

	s.logger.Info("Successfully removed the node", zap.String("addr", addr))

	return nil
}

func (s *Shrek) Leader() string {
	// TODO implement me
	panic("implement me")
}

func (s *Shrek) Stats() (map[string]interface{}, error) {
	dbInfo := map[string]interface{}{
		"dsn": s.dbConf.DSN,
	}

	if !s.dbConf.InMemory {
		dbInfo["path"] = s.dbConf.DBFilename
		stat, err := os.Stat(s.dbConf.DBFilename)
		if err != nil {
			return nil, err
		}
		dbInfo["size"] = stat.Size()
	} else {
		dbInfo["path"] = "in memory"
	}

	leaderID, err := s.LeaderID()
	if err != nil {
		return nil, err
	}

	raftLeaderAddr, _ := s.raft.LeaderWithID()

	stats := map[string]interface{}{
		"node_id":  s.raftID,
		"raft":     s.raft.Stats(),
		"raft_dir": s.raftDir,
		"addr":     s.Addr(),
		"leader": map[string]string{
			"node_id": leaderID,
			"addr":    string(raftLeaderAddr),
		},
		"apply_timeout":      s.ApplyTimeout.String(),
		"heartbeat_timeout":  s.HeartbeatTimeout.String(),
		"election_timeout":   s.ElectionTimeout.String(),
		"snapshot_threshold": s.SnapshotThreshold,
		"db_info":            dbInfo,
	}

	return stats, nil
}

type QueryRequest struct {
	Queries        []string
	IncludeTimings bool
	UseTx          bool
	Lvl            ConsistencyLevel
}

type ExecuteRequest struct {
	Queries        []string // slice of queries
	UseTx          bool     // either all queries will be executed successfully or it will as though none executed
	IncludeTimings bool     // timing information
}

// New ... Initializer
func New(ctx context.Context, cfg *config.Config, raftLayer Listener) *Shrek {
	logger := logging.WithContext(ctx)

	return &Shrek{
		ctx:               ctx,
		logger:            logger,
		raftLayer:         raftLayer,
		dbConf:            cfg.StorageConfig.DBCfg,
		meta:              make(map[string]map[string]string),
		raftID:            cfg.StorageConfig.RaftID,
		raftDir:           cfg.StorageConfig.RaftDir,
		ShutdownOnRemove:  cfg.StorageConfig.RaftShutdownOnRemove,
		SnapshotThreshold: cfg.StorageConfig.RaftSnapThreshold,
		ElectionTimeout:   cfg.StorageConfig.RaftElectionTimeout,
		HeartbeatTimeout:  cfg.StorageConfig.RaftHeartbeatTimeout,
		ApplyTimeout:      cfg.StorageConfig.RaftApplyTimeout,
		OpenTimeout:       cfg.StorageConfig.RaftOpenTimeout,
	}
}

func (s *Shrek) Open(allowSingle bool) error {
	s.logger.Info("Running Raft", zap.String("raftDir", s.raftDir), zap.String("NODE_ID", s.raftID))

	if err := os.MkdirAll(s.raftDir, 0755); err != nil {
		s.logger.Fatal("Failed to create raft dir")
		return err
	}

	// open database file or open it in-memory mode
	db, err := s.openDB()
	if err != nil {
		return err
	}

	// init db instance
	s.db = db

	isNewNode := !utils.PathExists(filepath.Join(s.raftDir, "raft.db"))

	// Create a transport layer
	trans := raft.NewNetworkTransport(
		NewRaftLayer(s.raftLayer),
		3,
		10*time.Second,
		os.Stderr,
	)

	s.raftTransport = trans

	// get raft config for the store
	raftCfg := s.getRaftConfig()
	raftCfg.LocalID = raft.ServerID(s.raftID)

	/*
		Snapshot stores the state to recover and restore data
			Example:
				If EC2 instance failed and an autoscaling group brought up another instance for the Raft server
				Rather than streaming all the data from the Raft leader, the new server would restore
				from the snapshot (which you could store in S3 or a similar storage service)
				and then get the latest changes from the leader
	*/
	snapshots, err := raft.NewFileSnapshotStore(
		s.raftDir,
		retainSnapshotCount,
		os.Stderr,
	)
	if err != nil {
		s.logger.Fatal("Error create file snapshot store", zap.Error(err))
		return err
	}

	// create the log store and stable store
	s.boltStore, err = raftboltdb.NewBoltStore(filepath.Join(s.raftDir, "raft.db"))
	if err != nil {
		s.logger.Fatal("Error create new bolt store", zap.Error(err))
		return err
	}
	s.raftStable = s.boltStore
	s.raftLog, err = raft.NewLogCache(512, s.boltStore)
	if err != nil {
		return err
	}

	// create the Raft instance and bootstrap the cluster
	rf, err := raft.NewRaft(
		raftCfg,
		s,
		s.raftLog,
		s.raftStable,
		snapshots,
		s.raftTransport,
	)
	if err != nil {
		s.logger.Fatal("Error creating raft system", zap.Error(err))
		return err
	}

	if allowSingle && isNewNode {
		s.logger.Debug("Running bootstrapping app...")
		s.logger.Debug("Opening store for node", zap.String("NODE_ID", s.raftID))
		cfg := raft.Configuration{
			Servers: []raft.Server{
				raft.Server{
					ID:      raftCfg.LocalID,
					Address: trans.LocalAddr(),
				},
			},
		}
		rf.BootstrapCluster(cfg)
	} else {
		s.logger.Debug("Bootstrapping app not needed")
	}

	// init raft instance
	s.raft = rf

	return nil
}

func (s *Shrek) openDB() (*sql.DB, error) {
	var db *sql.DB
	var err error
	if s.dbConf.InMemory {
		db, err = sql.OpenInMemoryWithDSN(s.dbConf.DSN)
		if err != nil {
			return nil, err
		}
		s.logger.Info("Successfully opened the in-memory database", zap.String("db path", s.dbConf.DBFilename))
	} else {
		if err := os.Remove(s.dbConf.DBFilename); err != nil && !os.IsNotExist(err) {
			return nil, err
		}
		db, err = sql.OpenWithDSN(s.dbConf.DBFilename, s.dbConf.DSN)
		if err != nil {
			return nil, err
		}
		s.logger.Info("Successfully opened the database", zap.String("db path", s.dbConf.DBFilename))
	}

	return db, nil
}

// LeaderID ... Returns leader ID
func (s *Shrek) LeaderID() (string, error) {
	addr, serverID := s.LeaderAddr()
	s.logger.Info("Current leader info", zap.String("addr", string(addr)), zap.String("server ID", string(serverID)))

	cfg := s.raft.GetConfiguration()
	if err := cfg.Error(); err != nil {
		return "", err
	}
	raftServers := cfg.Configuration().Servers

	for _, server := range raftServers {
		if server.Address == addr {
			return string(server.ID), nil
		}
	}

	return "", nil
}

func (s *Shrek) Shutdown() error {
	s.logger.Info("shutting down server")
	s.shutdownLock.Lock()
	defer s.shutdownLock.Unlock()

	if s.shutdown {
		return nil
	}

	s.shutdown = true

	if err := s.db.Close(); err != nil {
		return err
	}

	if s.raft != nil {
		s.raftTransport.Close()
		s.raftLayer.Close()
		future := s.raft.Shutdown()
		if err := future.Error(); err != nil {
			s.logger.Warn("error shutting down raft", zap.Error(err))
		}
	}

	return nil
}

func (s *Shrek) RegisterObserver(o *raft.Observer) {
	s.raft.RegisterObserver(o)
}

func (s *Shrek) DeregisterObserver(o *raft.Observer) {
	s.raft.DeregisterObserver(o)
}

func (s *Shrek) WaitForLeader(timeout time.Duration) (string, error) {
	tick := time.NewTicker(100 * time.Millisecond)
	defer tick.Stop()
	tmr := time.NewTimer(timeout)
	defer tmr.Stop()

	for {
		select {
		case <-tick.C:
			leader, _ := s.LeaderAddr()
			if leader != "" {
				return string(leader), nil
			}
		case <-tmr.C:
			return "", fmt.Errorf("timeout expired")
		}
	}
}

func (s *Shrek) WaitForAppliedIndex(idx uint64, timeout time.Duration) error {
	tick := time.NewTicker(100 * time.Millisecond)
	defer tick.Stop()
	tmr := time.NewTicker(timeout)
	defer tmr.Stop()

	for {
		select {
		case <-tick.C:
			if s.raft.AppliedIndex() >= idx {
				return nil
			}
		case <-tmr.C:
			return fmt.Errorf("timeout expired")
		}
	}
}

func (s *Shrek) WaitForApplied(timeout time.Duration) error {
	if timeout == 0 {
		return nil
	}
	s.logger.Info(
		"Waiting for the app of all Raft log entries to be completed on the database",
		zap.String("timeout", strconv.FormatInt(int64(timeout), 10)),
	)
	return s.WaitForAppliedIndex(s.raft.LastIndex(), timeout)
}

// ID ... Returns the raft ID
func (s *Shrek) ID() string {
	return s.raftID
}

// Addr ... Returns the address of the store
func (s *Shrek) Addr() string {
	return string(s.raftTransport.LocalAddr())
}

func (s *Shrek) LeaderAddr() (raft.ServerAddress, raft.ServerID) {
	return s.raft.LeaderWithID()
}

// Path ... Returns the path to the store's storage directory
func (s *Shrek) Path() string {
	return s.raftDir
}

// getRaftConfig ... Return raft config file
func (s *Shrek) getRaftConfig() *raft.Config {
	cfg := raft.DefaultConfig()
	cfg.ShutdownOnRemove = s.ShutdownOnRemove
	if s.SnapshotThreshold != 0 {
		cfg.SnapshotThreshold = s.SnapshotThreshold
	}
	if s.SnapshotInterval != 0 {
		cfg.SnapshotInterval = s.SnapshotInterval
	}
	if s.HeartbeatTimeout != 0 {
		cfg.HeartbeatTimeout = s.HeartbeatTimeout
	}
	if s.ElectionTimeout != 0 {
		cfg.ElectionTimeout = s.ElectionTimeout
	}
	return cfg
}
