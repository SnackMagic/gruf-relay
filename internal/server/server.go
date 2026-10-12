//go:generate mockgen -source=server.go -destination=server_mock.go -package=server
package server

import (
	"context"
	"fmt"
	"log/slog"
	"net"

	"github.com/bibendi/gruf-relay/internal/codec"
	"github.com/bibendi/gruf-relay/internal/config"
	"github.com/bibendi/gruf-relay/internal/log"
	"google.golang.org/grpc"
	"google.golang.org/grpc/encoding"
	"google.golang.org/grpc/keepalive"
)

type Proxy interface {
	HandleRequest(any, grpc.ServerStream) error
}

type Server struct {
	host      string
	port      int
	proxy     Proxy
	keepalive keepalive.ServerParameters
	opts      []grpc.ServerOption
}

func NewServer(cfg config.Server, proxy Proxy, opts ...grpc.ServerOption) *Server {
	return &Server{
		host:  cfg.Host,
		port:  cfg.Port,
		proxy: proxy,
		keepalive: keepalive.ServerParameters{
			MaxConnectionIdle:     cfg.Keepalive.MaxConnectionIdle,
			MaxConnectionAge:      cfg.Keepalive.MaxConnectionAge,
			MaxConnectionAgeGrace: cfg.Keepalive.MaxConnectionAgeGrace,
			Time:                  cfg.Keepalive.Time,
			Timeout:               cfg.Keepalive.Timeout,
		},
		opts: opts,
	}
}

func (s *Server) Serve(ctx context.Context) error {
	addr := fmt.Sprintf("%s:%d", s.host, s.port)
	log.Info("Starting gRPC server", slog.String("addr", addr),
		slog.Duration("keepalive_max_connection_idle", s.keepalive.MaxConnectionIdle),
		slog.Duration("keepalive_max_connection_age", s.keepalive.MaxConnectionAge),
		slog.Duration("keepalive_max_connection_age_grace", s.keepalive.MaxConnectionAgeGrace),
		slog.Duration("keepalive_time", s.keepalive.Time),
		slog.Duration("keepalive_timeout", s.keepalive.Timeout))

	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("failed to listen: %v", err)
	}

	encoding.RegisterCodec(codec.Codec())

	opts := append([]grpc.ServerOption{
		grpc.UnknownServiceHandler(s.proxy.HandleRequest),
		grpc.NumStreamWorkers(0),
		grpc.KeepaliveParams(s.keepalive),
	}, s.opts...)
	server := grpc.NewServer(opts...)

	errChan := make(chan error, 1)
	defer close(errChan)
	go func() {
		if err := server.Serve(lis); err != nil {
			errChan <- fmt.Errorf("gRPC server has failed: %v", err)
		}
	}()

	select {
	case err := <-errChan:
		return err
	case <-ctx.Done():
		log.Info("Stopping gRPC server")
		server.GracefulStop()
	}
	return nil
}
