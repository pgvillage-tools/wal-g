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

// hangingFolder simulates a storage endpoint that never answers uploads.
type hangingFolder struct {
	storage.Folder
}

func (f hangingFolder) GetSubFolder(path string) storage.Folder {
	return hangingFolder{f.Folder.GetSubFolder(path)}
}

func (f hangingFolder) PutObject(ctx context.Context, _ string, _ io.Reader) error {
	<-ctx.Done()
	return ctx.Err()
}

func TestTimeoutFolder_PutObjectTimesOut(t *testing.T) {
	folder := internal.NewTimeoutFolder(hangingFolder{memory.NewFolder("", memory.NewKVS())}, 20*time.Millisecond)

	start := time.Now()
	err := folder.GetSubFolder("sub").PutObject(context.Background(), "file", bytes.NewReader([]byte("data")))

	require.Error(t, err)
	assert.Contains(t, err.Error(), "did not finish within 20ms")
	assert.Less(t, time.Since(start), 5*time.Second)
}

func TestTimeoutFolder_ParentDeadlineIsNotBlamedOnFileTimeout(t *testing.T) {
	folder := internal.NewTimeoutFolder(hangingFolder{memory.NewFolder("", memory.NewKVS())}, time.Hour)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()

	err := folder.PutObject(ctx, "file", bytes.NewReader([]byte("data")))

	require.ErrorIs(t, err, context.DeadlineExceeded)
	assert.NotContains(t, err.Error(), "did not finish within")
}

func TestTimeoutFolder_PutObjectSucceeds(t *testing.T) {
	folder := internal.NewTimeoutFolder(memory.NewFolder("", memory.NewKVS()), time.Minute)

	require.NoError(t, folder.GetSubFolder("sub").PutObject(context.Background(), "file", bytes.NewReader([]byte("data"))))

	exists, err := folder.GetSubFolder("sub").Exists(context.Background(), "file")
	require.NoError(t, err)
	assert.True(t, exists)
}
