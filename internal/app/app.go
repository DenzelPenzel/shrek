package app

import (
	"context"
	"errors"
	"github.com/draculaas/shrek/internal/config"
	"github.com/draculaas/shrek/internal/logging"
	"github.com/draculaas/shrek/internal/network"
	"github.com/draculaas/shrek/internal/server"
	"github.com/draculaas/shrek/internal/shrek"
	"go.uber.org/zap"
	"os"
	"os/signal"
	"strings"
	"syscall"
)

type Application struct {
	cfg *config.Config
	ctx context.Context
	g   *shrek.Shrek
}

// New ... Initializer
func New(ctx context.Context, cfg *config.Config, g *shrek.Shrek) *Application {
	return &Application{
		ctx: ctx,
		cfg: cfg,
		g:   g,
	}
}

func setupStore(ctx context.Context, cfg *config.Config) (*shrek.Shrek, func(), error) {
	logger := logging.WithContext(ctx)
	// configure network layer
	nt := network.NewNetwork()
	raftAddr := cfg.StorageConfig.RaftAddr

	if err := nt.Open(raftAddr.String()); err != nil {
		logger.Fatal("failed to open network layer", zap.String("addr", raftAddr.String()), zap.Error(err))
	}

	// create a new store
	s := shrek.New(ctx, cfg, nt)

	// Determine join addresses, if necessary
	canJoin, err := s.JoinAllowed(cfg.StorageConfig.RaftDir)
	if err != nil {
		logger.Fatal("Unable to determine if join permitted", zap.Error(err))
	}

	var joins []string
	if canJoin {
		if cfg.StorageConfig.Join != "" {
			joins = strings.Split(cfg.StorageConfig.Join, ",")
		}
	}

	//  Initialize the Raft server
	if err := s.Open(len(joins) == 0); err != nil {
		logger.Fatal("Unable to open raft store", zap.Error(err))
	}

	apiAdvertise := cfg.ServerConfig.HttpAddr.String()
	if cfg.ServerConfig.HttpAdvertise.IP != nil {
		apiAdvertise = cfg.ServerConfig.HttpAdvertise.String()
	}

	meta := map[string]string{
		"api_addr": apiAdvertise,
	}

	if len(joins) > 0 {
		logger.Info("list of joining addresses", zap.String("address list", strings.Join(joins, ",")))
		raftAdvertise := cfg.StorageConfig.RaftAddr
		if cfg.StorageConfig.RaftAdvertise.IP != nil {
			raftAdvertise = cfg.StorageConfig.RaftAdvertise
		}
		// Join to the raft cluster
		joinedAddr, err := server.Join(joins, s.ID(), raftAdvertise, meta)
		if err != nil {
			logger.Fatal("Failed to join raft cluster", zap.Error(err))
		} else {
			logger.Info("Successfully joined raft cluster", zap.String("joined addr", joinedAddr))
		}
	} else {
		logger.Info("No join addresses specified")
	}

	// Wait until the store is in full consensus
	leader, err := s.WaitForLeader(s.OpenTimeout)
	if err != nil {
		logger.Fatal("Failed to achieve consensus", zap.Error(err))
		return nil, nil, err
	}

	logger.Info("Found the leader", zap.String("leader", leader))

	err = s.WaitForApplied(s.OpenTimeout)
	if err != nil {
		logger.Fatal("Can't apply last index", zap.Error(err))
		return nil, nil, err
	}

	if err := s.SetMetadata(meta); err != nil && !errors.Is(err, shrek.ErrNotLeader) {
		logger.Fatal("Unable to store metadata within the Raft consensus cluster", zap.Error(err))
	}

	stop := func() {
		logging.WithContext(ctx).Info("Starting to shutdown store")

		if err := s.Shutdown(); err != nil {
			logging.WithContext(ctx).Fatal("Failed to close store", zap.Error(err))
		}
	}

	return s, stop, nil
}

func setupHTTPServer(ctx context.Context, cfg *config.Config, s *shrek.Shrek) error {
	srv := server.New(ctx, cfg.ServerConfig, s)
	if err := srv.Run(); err != nil {
		return err
	}
	return nil
}

func NewApp(ctx context.Context, cfg *config.Config) (*Application, func(), error) {
	g, cleanup, err := setupStore(ctx, cfg)
	if err != nil {
		return nil, nil, err
	}

	// create HTTP server
	if err := setupHTTPServer(ctx, cfg, g); err != nil {
		return nil, nil, err
	}

	return New(ctx, cfg, g), cleanup, nil
}

// ListenForShutdown ... Handles and listens for shutdown
func (a *Application) ListenForShutdown(stop func()) {
	done := <-a.End() // Blocks until an OS signal is received

	logging.WithContext(a.ctx).
		Info("Received shutdown OS signal", zap.String("signal", done.String()))
	stop()
}

// End ... Returns a channel that will receive an OS signal
func (a *Application) End() <-chan os.Signal {
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	return sigs
}
