package s3

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"sync/atomic"
	"time"
)

// idleTimeoutConn fails reads and writes on a connection that makes no progress for timeout.
//
// Every Read and Write moves both deadlines, so a request only fails when nothing is sent or received for timeout:
// while sending a request body to a server that stopped reading it, while waiting for response headers or while
// receiving a response body. The resulting "i/o timeout" error is retried by the S3 retryer, on a new connection.
// Time the caller spends between reads of a response body (e.g. writing it to disk) does not count, because the
// deadline is only set when a Read starts.
type idleTimeoutConn struct {
	net.Conn
	timeout  time.Duration
	timedOut atomic.Bool
}

func (c *idleTimeoutConn) Read(p []byte) (int, error) {
	if err := c.Conn.SetReadDeadline(time.Now().Add(c.timeout)); err != nil {
		return 0, c.explain(err)
	}
	n, err := c.Conn.Read(p)
	return n, c.explain(err)
}

func (c *idleTimeoutConn) Write(p []byte) (int, error) {
	deadline := time.Now().Add(c.timeout)
	if err := c.Conn.SetWriteDeadline(deadline); err != nil {
		return 0, c.explain(err)
	}
	// The HTTP transport has a Read pending on the connection while the request is written, waiting for the
	// response. Sending data is progress, so move its deadline too (deadlines apply to pending calls).
	if err := c.Conn.SetReadDeadline(deadline); err != nil {
		return 0, c.explain(err)
	}
	n, err := c.Conn.Write(p)
	return n, c.explain(err)
}

// explain reports every error after an idle timeout as a timeout. When the read side times out, the HTTP transport
// closes the connection, so a blocked write fails with "use of closed network connection" instead. A timeout error
// keeps it recognizable (and retryable) as a transient network error.
func (c *idleTimeoutConn) explain(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, os.ErrDeadlineExceeded) {
		c.timedOut.Store(true)
	} else if !c.timedOut.Load() {
		return err
	}
	return fmt.Errorf("%w: no data sent or received for %s", os.ErrDeadlineExceeded, c.timeout)
}

type dialContextFunc func(ctx context.Context, network, address string) (net.Conn, error)

// withIdleTimeout wraps every connection made by dial in an idleTimeoutConn.
func withIdleTimeout(dial dialContextFunc, timeout time.Duration) dialContextFunc {
	return func(ctx context.Context, network, address string) (net.Conn, error) {
		conn, err := dial(ctx, network, address)
		if err != nil {
			return nil, err
		}
		return &idleTimeoutConn{Conn: conn, timeout: timeout}, nil
	}
}
