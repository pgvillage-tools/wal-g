package pg

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	conf "github.com/wal-g/wal-g/internal/config"
)

func TestGetCommandTimeout(t *testing.T) {
	tests := []struct {
		name    string
		setting string
		args    []string
		want    time.Duration
		wantErr bool
	}{
		{name: "disabled by default", want: 0},
		{name: "from setting", setting: "90s", want: 90 * time.Second},
		{name: "setting -1 disables", setting: "-1", want: 0},
		{name: "flag overrides setting", setting: "90s", args: []string{"--timeout", "10s"}, want: 10 * time.Second},
		{name: "flag -1 disables setting", setting: "90s", args: []string{"--timeout", "-1"}, want: 0},
		{name: "invalid setting", setting: "soon", wantErr: true},
		{name: "invalid flag", args: []string{"--timeout", "10"}, wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.setting != "" {
				viper.Set(conf.PgWalPushTimeout, tt.setting)
				defer viper.Set(conf.PgWalPushTimeout, nil)
			}
			flagValue := conf.NoTimeout
			cmd := &cobra.Command{}
			cmd.Flags().StringVar(&flagValue, timeoutFlag, conf.NoTimeout, "")
			require.NoError(t, cmd.Flags().Parse(tt.args))

			got, err := getCommandTimeout(cmd, flagValue, conf.PgWalPushTimeout)
			if tt.wantErr {
				assert.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestWithCommandTimeout(t *testing.T) {
	ctx, cancel := withCommandTimeout(context.Background(), "test", 0)
	_, hasDeadline := ctx.Deadline()
	assert.False(t, hasDeadline)
	cancel()

	ctx, cancel = withCommandTimeout(context.Background(), "test", 10*time.Millisecond)
	defer cancel()
	<-ctx.Done()
	assert.True(t, errors.Is(ctx.Err(), context.DeadlineExceeded))
}
