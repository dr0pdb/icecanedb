package icecanekv

const (
	// KB - Kilobytes
	KB uint64 = 1024

	// MB - Megabytes
	MB uint64 = 1024 * 1024
)

// KVConfig defines the configuration settings for IcecaneKV
type KVConfig struct {
	DbPath   string
	LogLevel string
}

// NewDefaultKVConfig returns a new default key vault configuration.
func NewDefaultKVConfig() *KVConfig {
	return &KVConfig{
		DbPath:   "/tmp/icecane",
		LogLevel: "info",
	}
}

// Validate validates a KVConfig and returns an error if it's invalid.
func (conf *KVConfig) Validate() error {
	// todo: implement this
	return nil
}
