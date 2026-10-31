package pg

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/spf13/cobra"
	"github.com/wal-g/tracelog"
	conf "github.com/wal-g/wal-g/internal/config"
)

const timeoutFlag = "timeout"

func timeoutFlagDescription(command, setting string) string {
	return fmt.Sprintf("Maximum duration of the whole %s run (e.g. 30s, 5m, 2h). "+
		"%s disables the limit. Overrides %s.", command, conf.NoTimeout, setting)
}

// getCommandTimeout returns the --timeout flag value when it is set explicitly,
// otherwise the value of the given setting. 0 means no timeout.
func getCommandTimeout(cmd *cobra.Command, flagValue, setting string) (time.Duration, error) {
	if cmd.Flags().Changed(timeoutFlag) {
		timeout, err := conf.ParseTimeout(flagValue)
		if err != nil {
			return 0, fmt.Errorf("invalid --%s flag: %w", timeoutFlag, err)
		}
		return timeout, nil
	}
	return conf.GetTimeoutSetting(setting)
}

// withCommandTimeout bounds ctx by timeout (when > 0) and logs once the deadline is hit,
// so it is clear why in-flight operations start failing.
func withCommandTimeout(ctx context.Context, command string, timeout time.Duration) (context.Context, context.CancelFunc) {
	if timeout <= 0 {
		return context.WithCancel(ctx)
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	stop := context.AfterFunc(ctx, func() {
		if errors.Is(ctx.Err(), context.DeadlineExceeded) {
			tracelog.ErrorLogger.Printf("%s did not finish within %s, cancelling", command, timeout)
		}
	})
	return ctx, func() {
		stop()
		cancel()
	}
}
