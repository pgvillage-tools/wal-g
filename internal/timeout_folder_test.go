package internal_test

import (
	"bytes"
	"context"
	"io"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/pkg/storages/memory"
	"github.com/wal-g/wal-g/pkg/storages/storage"
)

const shortTimeout = 50 * time.Millisecond

// hangingFolder simulates a storage endpoint that accepts requests but never answers them.
type hangingFolder struct {
	storage.Folder
}

func newHangingFolder() hangingFolder {
	return hangingFolder{memory.NewFolder("", memory.NewKVS())}
}

func (f hangingFolder) GetSubFolder(path string) storage.Folder {
	return hangingFolder{f.Folder.GetSubFolder(path)}
}

func (f hangingFolder) PutObject(ctx context.Context, _ string, _ io.Reader) error {
	<-ctx.Done()
	return ctx.Err()
}

func (f hangingFolder) ListFolder(ctx context.Context) ([]storage.Object, []storage.Folder, error) {
	<-ctx.Done()
	return nil, nil, ctx.Err()
}

func (f hangingFolder) Exists(ctx context.Context, _ string) (bool, error) {
	<-ctx.Done()
	return false, ctx.Err()
}

func (f hangingFolder) ReadObject(ctx context.Context, _ string) (io.ReadCloser, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}

func (f hangingFolder) ListFolderSegment(ctx context.Context, _, _ *string) ([]storage.Object, []storage.Folder, error) {
	return f.ListFolder(ctx)
}

// stallingFolder returns readers that deliver one chunk and then stall until the request is cancelled,
// like an HTTP response body whose server stops sending data.
type stallingFolder struct {
	storage.Folder
}

func (f stallingFolder) ReadObject(ctx context.Context, _ string) (io.ReadCloser, error) {
	return &stallingReader{ctx: ctx}, nil
}

type stallingReader struct {
	ctx  context.Context
	sent bool
}

func (r *stallingReader) Read(p []byte) (int, error) {
	if !r.sent {
		r.sent = true
		return copy(p, "chunk"), nil
	}
	<-r.ctx.Done()
	return 0, r.ctx.Err()
}

func (r *stallingReader) Close() error { return nil }

func assertTimesOut(t *testing.T, err error, msg string, start time.Time) {
	t.Helper()
	require.Error(t, err)
	assert.Contains(t, err.Error(), msg)
	assert.Less(t, time.Since(start), 5*time.Second)
}

func TestTimeoutFolder_UploadTimeout(t *testing.T) {
	folder := internal.NewTimeoutFolder(newHangingFolder(), shortTimeout, 0)

	start := time.Now()
	err := folder.GetSubFolder("sub").PutObject(context.Background(), "file", bytes.NewReader([]byte("data")))
	assertTimesOut(t, err, "upload of 'file' did not finish within 50ms", start)
}

func TestTimeoutFolder_OperationTimeouts(t *testing.T) {
	folder := internal.NewTimeoutFolder(newHangingFolder(), 0, shortTimeout).GetSubFolder("sub")
	ctx := context.Background()

	start := time.Now()
	_, _, err := folder.ListFolder(ctx)
	assertTimesOut(t, err, "listing of 'sub/' did not finish within 50ms", start)

	start = time.Now()
	_, err = folder.Exists(ctx, "file")
	assertTimesOut(t, err, "existence check of 'file' did not finish within 50ms", start)

	start = time.Now()
	_, err = folder.ReadObject(ctx, "file")
	assertTimesOut(t, err, "opening 'file' did not finish within 50ms", start)

	ext, ok := folder.(storage.FolderExt)
	require.True(t, ok, "FolderExt of the wrapped folder must stay available")
	start = time.Now()
	_, _, err = ext.ListFolderSegment(ctx, nil, nil)
	assertTimesOut(t, err, "listing of 'sub/' did not finish within 50ms", start)
}

func TestTimeoutFolder_FolderExtOnlyWhenSupported(t *testing.T) {
	folder := internal.NewTimeoutFolder(memory.NewFolder("", memory.NewKVS()), 0, time.Minute)
	_, ok := folder.(storage.FolderExt)
	assert.False(t, ok)
}

func TestTimeoutFolder_ReadStall(t *testing.T) {
	folder := internal.NewTimeoutFolder(stallingFolder{memory.NewFolder("", memory.NewKVS())}, 0, shortTimeout)

	reader, err := folder.ReadObject(context.Background(), "file")
	require.NoError(t, err)
	defer reader.Close()

	start := time.Now()
	data, err := io.ReadAll(reader)
	assert.Equal(t, "chunk", string(data))
	assertTimesOut(t, err, "reading 'file' got no data within 50ms", start)
}

func TestTimeoutFolder_SlowConsumerIsNotATimeout(t *testing.T) {
	inner := memory.NewFolder("", memory.NewKVS())
	require.NoError(t, inner.PutObject(context.Background(), "file", bytes.NewReader([]byte("some data"))))
	folder := internal.NewTimeoutFolder(inner, 0, shortTimeout)

	reader, err := folder.ReadObject(context.Background(), "file")
	require.NoError(t, err)
	defer reader.Close()

	// The caller taking long between reads (e.g. writing to a slow disk) must not count.
	time.Sleep(3 * shortTimeout)
	buf := make([]byte, 4)
	_, err = io.ReadFull(reader, buf)
	require.NoError(t, err)
	time.Sleep(3 * shortTimeout)
	rest, err := io.ReadAll(reader)
	require.NoError(t, err)
	assert.Equal(t, "some data", string(buf)+string(rest))
}

func TestTimeoutFolder_ParentDeadlineIsNotBlamedOnOwnTimeout(t *testing.T) {
	folder := internal.NewTimeoutFolder(newHangingFolder(), time.Hour, time.Hour)
	ctx, cancel := context.WithTimeout(context.Background(), shortTimeout)
	defer cancel()

	err := folder.PutObject(ctx, "file", bytes.NewReader([]byte("data")))
	require.ErrorIs(t, err, context.DeadlineExceeded)
	assert.NotContains(t, err.Error(), "did not finish within")

	_, _, err = folder.ListFolder(ctx)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	assert.NotContains(t, err.Error(), "did not finish within")
}

func TestTimeoutFolder_OperationsSucceed(t *testing.T) {
	folder := internal.NewTimeoutFolder(memory.NewFolder("", memory.NewKVS()), time.Minute, time.Minute)
	ctx := context.Background()

	require.NoError(t, folder.GetSubFolder("sub").PutObject(ctx, "file", bytes.NewReader([]byte("data"))))

	exists, err := folder.GetSubFolder("sub").Exists(ctx, "file")
	require.NoError(t, err)
	assert.True(t, exists)

	objects, subFolders, err := folder.ListFolder(ctx)
	require.NoError(t, err)
	assert.Empty(t, objects)
	require.Len(t, subFolders, 1)
	_, isWrapped := subFolders[0].(*internal.TimeoutFolder)
	assert.True(t, isWrapped, "subfolders from a listing must keep the timeouts")

	reader, err := folder.GetSubFolder("sub").ReadObject(ctx, "file")
	require.NoError(t, err)
	data, err := io.ReadAll(reader)
	require.NoError(t, err)
	require.NoError(t, reader.Close())
	assert.Equal(t, "data", string(data))
}
