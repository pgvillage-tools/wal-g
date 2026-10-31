package pg

import (
	"context"
	"errors"
	"fmt"

	"github.com/spf13/cobra"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	conf "github.com/wal-g/wal-g/internal/config"
	"github.com/wal-g/wal-g/internal/databases/postgres"
)

const WalPushShortDescription = "Uploads a WAL file to storage"

var walPushTimeout string

// walPushCmd represents the walPush command
var walPushCmd = &cobra.Command{
	Use:   "wal-push wal_filepath",
	Short: WalPushShortDescription, // TODO : improve description
	Args:  cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		timeout, err := getCommandTimeout(cmd, walPushTimeout, conf.PgWalPushTimeout)
		tracelog.ErrorLogger.FatalOnError(err)

		ctx, cancel := withCommandTimeout(cmd.Context(), "wal-push", timeout)
		defer cancel()

		err = runWalPush(ctx, args[0])
		if err != nil && errors.Is(ctx.Err(), context.DeadlineExceeded) {
			err = fmt.Errorf("wal-push of '%s' did not finish within %s: %w", args[0], timeout, err)
		}
		tracelog.ErrorLogger.FatalOnError(err)
	},
}

func runWalPush(ctx context.Context, walFilePath string) error {
	storage, err := internal.ConfigureMultiStorage(ctx, true)
	if err != nil {
		return fmt.Errorf("failed to configure multi-storage: %w", err)
	}

	walUploader, err := postgres.PrepareMultiStorageWalUploader(ctx, storage.RootFolder(), targetStorage)
	if err != nil {
		return err
	}

	return postgres.HandleWALPush(ctx, walUploader, walFilePath)
}

func init() {
	Cmd.AddCommand(walPushCmd)
	walPushCmd.Flags().StringVar(&targetStorage, "target-storage", "", targetStorageDescription)
	walPushCmd.Flags().StringVar(&walPushTimeout, timeoutFlag, conf.NoTimeout,
		timeoutFlagDescription("wal-push", conf.PgWalPushTimeout))
}
