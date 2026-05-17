package metrics

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// DefaultAddr is the loopback-only default address the operator should
// stick with unless they have a specific reason to override it.
const DefaultAddr = "127.0.0.1:9090"

// ListenAndServe binds `addr` and serves `/metrics` over plain HTTP
// using the default Prometheus gatherer. The HTTP server is shut down
// gracefully when `ctx` is cancelled.
//
// `addr` MUST resolve to a loopback interface in production. The
// function does not enforce this — operators may need to expose
// metrics over a private network behind a reverse proxy — but the
// package documentation calls out the policy and the default in
// DefaultAddr is loopback.
func ListenAndServe(ctx context.Context, addr string) error {
	if addr == "" {
		addr = DefaultAddr
	}
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(prometheus.DefaultGatherer, promhttp.HandlerOpts{}))

	srv := &http.Server{
		Addr:              addr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("metrics: listen on %s: %w", addr, err)
	}

	errCh := make(chan error, 1)
	go func() {
		serveErr := srv.Serve(ln)
		if serveErr != nil && !errors.Is(serveErr, http.ErrServerClosed) {
			errCh <- serveErr
			return
		}
		errCh <- nil
	}()

	select {
	case <-ctx.Done():
		shutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutCtx)
		<-errCh
		return nil
	case err := <-errCh:
		return err
	}
}
