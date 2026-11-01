package setting

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

// TODO: Instead of reading settings from viper as strings and then parsing them here, read them already parsed using
// functions such as internal.GetBoolSettingDefault.

func BoolOptional(settings map[string]string, key string, defaultVal bool) (bool, error) {
	strVal, ok := settings[key]
	if ok {
		val, err := strconv.ParseBool(strVal)
		if err != nil {
			return false, fmt.Errorf("setting %q must be a boolean: %w", key, err)
		}
		return val, nil
	}
	return defaultVal, nil
}

func IntOptional(settings map[string]string, key string, defaultVal int) (int, error) {
	strVal, ok := settings[key]
	if ok {
		val, err := strconv.Atoi(strVal)
		if err != nil {
			return 0, fmt.Errorf("setting %q must be an integer: %w", key, err)
		}
		return val, nil
	}
	return defaultVal, nil
}

func Int(settings map[string]string, key string) (int, error) {
	strVal, ok := settings[key]
	if ok {
		val, err := strconv.Atoi(strVal)
		if err != nil {
			return 0, fmt.Errorf("setting %q must be an integer: %w", key, err)
		}
		return val, nil
	}
	return 0, fmt.Errorf("setting %q is required", key)
}

func Int64Optional(settings map[string]string, key string, defaultVal int64) (int64, error) {
	strVal, ok := settings[key]
	if ok {
		val, err := strconv.ParseInt(strVal, 10, 64)
		if err != nil {
			return 0, fmt.Errorf("setting %q must be a 64-bit integer: %w", key, err)
		}
		return val, nil
	}
	return defaultVal, nil
}

// TimeoutOptional parses a timeout such as "30s" or "5m". "-1", "0" and negative values mean "no timeout"
// and are returned as 0.
func TimeoutOptional(settings map[string]string, key string) (time.Duration, error) {
	strVal := strings.TrimSpace(settings[key])
	if strVal == "" || strVal == "-1" || strVal == "0" {
		return 0, nil
	}
	val, err := time.ParseDuration(strVal)
	if err != nil {
		return 0, fmt.Errorf("setting %q must be a duration (e.g. 30s, 5m) or -1: %w", key, err)
	}
	if val < 0 {
		return 0, nil
	}
	return val, nil
}

func FirstDefined(settings map[string]string, keys ...string) string {
	for _, key := range keys {
		if value, ok := settings[key]; ok {
			return value
		}
	}
	return ""
}
