package internal

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync/atomic"
	"time"

	"github.com/wal-g/wal-g/pkg/storages/storage"
)

// TimeoutFolder bounds the duration of storage operations on the underlying folder. It protects against storage
// calls that hang forever, e.g. when the storage endpoint accepts connections but never responds.
//
//   - uploadTimeout limits every PutObject call.
//   - operationTimeout limits every other call that talks to the storage (listing, existence checks, stat, delete,
//     copy, validation). For ReadObject it limits opening the object and every single read of its content, so a
//     download may take longer as a whole, as long as the storage keeps sending data.
//
// A zero timeout disables the corresponding limit.
type TimeoutFolder struct {
	storage.Folder
	uploadTimeout    time.Duration
	operationTimeout time.Duration
}

// timeoutFolderExt keeps storage.FolderExt available when the wrapped folder supports it.
type timeoutFolderExt struct {
	*TimeoutFolder
}

// NewTimeoutFolder wraps the folder. The result implements storage.FolderExt if the folder does.
func NewTimeoutFolder(folder storage.Folder, uploadTimeout, operationTimeout time.Duration) storage.Folder {
	tf := &TimeoutFolder{Folder: folder, uploadTimeout: uploadTimeout, operationTimeout: operationTimeout}
	if _, ok := folder.(storage.FolderExt); ok {
		return timeoutFolderExt{tf}
	}
	return tf
}

// withTimeout runs op with ctx bounded by timeout (if > 0) and explains in the error when the timeout was hit.
func withTimeout(ctx context.Context, timeout time.Duration, what string, op func(ctx context.Context) error) error {
	if timeout <= 0 {
		return op(ctx)
	}
	opCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	err := op(opCtx)
	return timeoutError(ctx, opCtx, err, what, timeout)
}

// timeoutError only blames our own timeout when the caller's context is still fine.
func timeoutError(parent, opCtx context.Context, err error, what string, timeout time.Duration) error {
	if err != nil && parent.Err() == nil && errors.Is(opCtx.Err(), context.DeadlineExceeded) {
		return fmt.Errorf("%s did not finish within %s: %w", what, timeout, err)
	}
	return err
}

func (tf *TimeoutFolder) GetSubFolder(subFolderRelativePath string) storage.Folder {
	return NewTimeoutFolder(tf.Folder.GetSubFolder(subFolderRelativePath), tf.uploadTimeout, tf.operationTimeout)
}

func (tf *TimeoutFolder) PutObject(ctx context.Context, name string, content io.Reader) error {
	return withTimeout(ctx, tf.uploadTimeout, fmt.Sprintf("upload of '%s'", name), func(ctx context.Context) error {
		return tf.Folder.PutObject(ctx, name, content)
	})
}

func (tf *TimeoutFolder) ListFolder(ctx context.Context) (objects []storage.Object, subFolders []storage.Folder, err error) {
	err = withTimeout(ctx, tf.operationTimeout, fmt.Sprintf("listing of '%s'", tf.GetPath()), func(ctx context.Context) error {
		objects, subFolders, err = tf.Folder.ListFolder(ctx)
		return err
	})
	return objects, tf.wrapSubFolders(subFolders), err
}

func (tf *TimeoutFolder) DeleteObjects(ctx context.Context, objects []storage.Object) error {
	what := fmt.Sprintf("deletion of %d objects in '%s'", len(objects), tf.GetPath())
	return withTimeout(ctx, tf.operationTimeout, what, func(ctx context.Context) error {
		return tf.Folder.DeleteObjects(ctx, objects)
	})
}

func (tf *TimeoutFolder) Exists(ctx context.Context, objectRelativePath string) (exists bool, err error) {
	err = withTimeout(ctx, tf.operationTimeout, fmt.Sprintf("existence check of '%s'", objectRelativePath),
		func(ctx context.Context) error {
			exists, err = tf.Folder.Exists(ctx, objectRelativePath)
			return err
		})
	return exists, err
}

func (tf *TimeoutFolder) StatObject(ctx context.Context, objectRelativePath string) (object storage.Object, err error) {
	err = withTimeout(ctx, tf.operationTimeout, fmt.Sprintf("stat of '%s'", objectRelativePath),
		func(ctx context.Context) error {
			object, err = tf.Folder.StatObject(ctx, objectRelativePath)
			return err
		})
	return object, err
}

func (tf *TimeoutFolder) CopyObject(ctx context.Context, srcPath string, dstPath string) error {
	what := fmt.Sprintf("copy of '%s' to '%s'", srcPath, dstPath)
	return withTimeout(ctx, tf.operationTimeout, what, func(ctx context.Context) error {
		return tf.Folder.CopyObject(ctx, srcPath, dstPath)
	})
}

func (tf *TimeoutFolder) Validate(ctx context.Context) error {
	return withTimeout(ctx, tf.operationTimeout, fmt.Sprintf("validation of '%s'", tf.GetPath()),
		func(ctx context.Context) error {
			return tf.Folder.Validate(ctx)
		})
}

func (tf *TimeoutFolder) SetVersioningEnabled(ctx context.Context, enable bool) {
	_ = withTimeout(ctx, tf.operationTimeout, "", func(ctx context.Context) error {
		tf.Folder.SetVersioningEnabled(ctx, enable)
		return nil
	})
}

func (tf *TimeoutFolder) GetVersioningEnabled(ctx context.Context) (enabled bool) {
	_ = withTimeout(ctx, tf.operationTimeout, "", func(ctx context.Context) error {
		enabled = tf.Folder.GetVersioningEnabled(ctx)
		return nil
	})
	return enabled
}

// ReadObject limits opening the object and every single Read of its content by operationTimeout.
// The time the caller spends between reads (e.g. writing the data to disk) does not count.
func (tf *TimeoutFolder) ReadObject(ctx context.Context, objectRelativePath string) (io.ReadCloser, error) {
	if tf.operationTimeout <= 0 {
		return tf.Folder.ReadObject(ctx, objectRelativePath)
	}
	readCtx, cancel := context.WithCancel(ctx)
	timer := time.AfterFunc(tf.operationTimeout, cancel)
	reader, err := tf.Folder.ReadObject(readCtx, objectRelativePath)
	timedOut := !timer.Stop()
	if err != nil || timedOut {
		cancel()
		if err == nil {
			_ = reader.Close()
			err = context.DeadlineExceeded
		}
		if timedOut && ctx.Err() == nil {
			return nil, fmt.Errorf("opening '%s' did not finish within %s: %w", objectRelativePath, tf.operationTimeout, err)
		}
		return nil, err
	}
	return &stallTimeoutReader{
		reader: reader, parent: ctx, cancel: cancel, timer: timer,
		timeout: tf.operationTimeout, name: objectRelativePath,
	}, nil
}

// stallTimeoutReader cancels the read when a single Read call takes longer than timeout.
type stallTimeoutReader struct {
	reader   io.ReadCloser
	parent   context.Context
	cancel   context.CancelFunc
	timer    *time.Timer
	timeout  time.Duration
	name     string
	timedOut atomic.Bool
}

func (r *stallTimeoutReader) Read(p []byte) (int, error) {
	if r.timedOut.Load() {
		return 0, r.stallError(context.DeadlineExceeded)
	}
	r.timer.Reset(r.timeout)
	n, err := r.reader.Read(p)
	if !r.timer.Stop() {
		// The timer fired during this Read and cancelled the request.
		r.timedOut.Store(true)
		if err == nil || err == io.EOF {
			err = context.DeadlineExceeded
		}
		return n, r.stallError(err)
	}
	return n, err
}

func (r *stallTimeoutReader) stallError(err error) error {
	if r.parent.Err() != nil {
		return err
	}
	return fmt.Errorf("reading '%s' got no data within %s: %w", r.name, r.timeout, err)
}

func (r *stallTimeoutReader) Close() error {
	r.timer.Stop()
	defer r.cancel()
	return r.reader.Close()
}

func (tf *TimeoutFolder) wrapSubFolders(subFolders []storage.Folder) []storage.Folder {
	for i, subFolder := range subFolders {
		subFolders[i] = NewTimeoutFolder(subFolder, tf.uploadTimeout, tf.operationTimeout)
	}
	return subFolders
}

// SetShowAllVersions delegates the "show all versions" toggle to the underlying folder (if supported).
func (tf *TimeoutFolder) SetShowAllVersions(show bool) {
	storage.SetShowAllVersions(tf.Folder, show)
}

func (tf timeoutFolderExt) ListFolderSegment(
	ctx context.Context, startAfter *string, endBefore *string,
) (objects []storage.Object, subFolders []storage.Folder, err error) {
	ext := tf.Folder.(storage.FolderExt)
	err = withTimeout(ctx, tf.operationTimeout, fmt.Sprintf("listing of '%s'", tf.GetPath()),
		func(ctx context.Context) error {
			objects, subFolders, err = ext.ListFolderSegment(ctx, startAfter, endBefore)
			return err
		})
	return objects, tf.wrapSubFolders(subFolders), err
}
