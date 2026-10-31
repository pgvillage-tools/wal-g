package internal

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/wal-g/wal-g/pkg/storages/storage"
)

// TimeoutFolder bounds the duration of every single object upload (PutObject)
// to the underlying folder. It protects against storage calls that hang forever,
// e.g. when the storage endpoint accepts connections but never responds.
type TimeoutFolder struct {
	storage.Folder
	timeout time.Duration
}

func NewTimeoutFolder(folder storage.Folder, timeout time.Duration) *TimeoutFolder {
	return &TimeoutFolder{Folder: folder, timeout: timeout}
}

func (tf *TimeoutFolder) GetSubFolder(subFolderRelativePath string) storage.Folder {
	return NewTimeoutFolder(tf.Folder.GetSubFolder(subFolderRelativePath), tf.timeout)
}

func (tf *TimeoutFolder) PutObject(ctx context.Context, name string, content io.Reader) error {
	uploadCtx, cancel := context.WithTimeout(ctx, tf.timeout)
	defer cancel()
	err := tf.Folder.PutObject(uploadCtx, name, content)
	// Only blame the per-file timeout when the caller's own context is still fine.
	if err != nil && ctx.Err() == nil && errors.Is(uploadCtx.Err(), context.DeadlineExceeded) {
		return fmt.Errorf("upload of '%s' did not finish within %s: %w", name, tf.timeout, err)
	}
	return err
}

// SetShowAllVersions delegates the "show all versions" toggle to the underlying folder (if supported).
func (tf *TimeoutFolder) SetShowAllVersions(show bool) {
	storage.SetShowAllVersions(tf.Folder, show)
}
