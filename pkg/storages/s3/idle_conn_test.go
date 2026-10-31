package s3

import (
	"bytes"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const testIdleTimeout = 300 * time.Millisecond

func newIdleTimeoutClient() *http.Client {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	dialer := &net.Dialer{Timeout: 5 * time.Second}
	transport.DialContext = withIdleTimeout(dialer.DialContext, testIdleTimeout)
	return &http.Client{Transport: transport}
}

// startBlackhole accepts connections but never reads from or writes to them.
func startBlackhole(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	var conns []net.Conn
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			conns = append(conns, conn)
		}
	}()
	t.Cleanup(func() {
		_ = listener.Close()
		for _, conn := range conns {
			_ = conn.Close()
		}
	})
	return "http://" + listener.Addr().String()
}

func TestIdleTimeout_UploadToServerThatStopsReading(t *testing.T) {
	url := startBlackhole(t)
	body := bytes.Repeat([]byte("x"), 64<<20) // more than the socket buffers hold

	start := time.Now()
	_, err := newIdleTimeoutClient().Post(url, "application/octet-stream", bytes.NewReader(body))

	require.Error(t, err)
	assert.Contains(t, err.Error(), "i/o timeout")
	assert.Less(t, time.Since(start), 5*time.Second)
}

func TestIdleTimeout_ServerNeverAnswers(t *testing.T) {
	url := startBlackhole(t)

	start := time.Now()
	_, err := newIdleTimeoutClient().Get(url)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "i/o timeout")
	assert.Less(t, time.Since(start), 5*time.Second)
}

func TestIdleTimeout_DownloadStalls(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("first chunk"))
		w.(http.Flusher).Flush()
		<-r.Context().Done()
	}))
	defer server.Close()

	resp, err := newIdleTimeoutClient().Get(server.URL)
	require.NoError(t, err)
	defer resp.Body.Close()

	start := time.Now()
	_, err = io.ReadAll(resp.Body)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "i/o timeout")
	assert.Less(t, time.Since(start), 5*time.Second)
}

func TestIdleTimeout_SlowButProgressingDownloadSucceeds(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		for i := 0; i < 8; i++ { // takes ~4x the idle timeout in total
			_, _ = w.Write([]byte("chunk"))
			w.(http.Flusher).Flush()
			time.Sleep(testIdleTimeout / 2)
		}
	}))
	defer server.Close()

	resp, err := newIdleTimeoutClient().Get(server.URL)
	require.NoError(t, err)
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	assert.Len(t, data, 8*len("chunk"))
}

func TestIdleTimeout_SlowConsumerSucceeds(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("some data"))
	}))
	defer server.Close()
	client := newIdleTimeoutClient()

	resp, err := client.Get(server.URL)
	require.NoError(t, err)
	defer resp.Body.Close()

	// The caller taking long between reads (e.g. writing to a slow disk) must not count.
	buf := make([]byte, 4)
	time.Sleep(2 * testIdleTimeout)
	_, err = io.ReadFull(resp.Body, buf)
	require.NoError(t, err)
	time.Sleep(2 * testIdleTimeout)
	rest, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	assert.Equal(t, "some data", string(buf)+string(rest))

	// A pooled connection that was idle for longer than the timeout must not break the next request.
	time.Sleep(2 * testIdleTimeout)
	resp2, err := client.Get(server.URL)
	require.NoError(t, err)
	defer resp2.Body.Close()
	data, err := io.ReadAll(resp2.Body)
	require.NoError(t, err)
	assert.Equal(t, "some data", string(data))
}
