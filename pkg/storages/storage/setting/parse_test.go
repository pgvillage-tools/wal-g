package setting

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestTimeoutOptional(t *testing.T) {
	for value, want := range map[string]time.Duration{
		"":     0,
		"-1":   0,
		"0":    0,
		"-5m":  0,
		"30s":  30 * time.Second,
		" 5m ": 5 * time.Minute,
	} {
		got, err := TimeoutOptional(map[string]string{"KEY": value}, "KEY")
		assert.NoError(t, err, value)
		assert.Equal(t, want, got, value)
	}

	got, err := TimeoutOptional(map[string]string{}, "KEY")
	assert.NoError(t, err)
	assert.Zero(t, got)

	for _, value := range []string{"5", "soon"} {
		_, err := TimeoutOptional(map[string]string{"KEY": value}, "KEY")
		assert.Error(t, err, value)
	}
}
