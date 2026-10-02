package dunnelean

import "time"

type settings struct {
	running, queued int
	shutdown        time.Duration
	cache           string
}

// Option configures an Engine before opening its state database.
type Option func(*settings) error

func invalid(message string) error { return &Error{Code: "INVALID_CONFIG", Message: message} }
func WithMaxRunning(n int) Option {
	return func(s *settings) error {
		if n < 1 || n > 64 {
			return invalid("max_running must be 1..64")
		}
		s.running = n
		return nil
	}
}
func WithMaxQueued(n int) Option {
	return func(s *settings) error {
		if n < 0 || n > 4096 {
			return invalid("max_queued must be 0..4096")
		}
		s.queued = n
		return nil
	}
}
func WithShutdownTimeout(d time.Duration) Option {
	return func(s *settings) error {
		if d < 0 {
			return invalid("shutdown timeout must be nonnegative")
		}
		s.shutdown = d
		return nil
	}
}

// WithNativeCacheDir chooses a writable directory for verified bundled libraries.
// The filesystem must permit dynamic library loading (noexec mounts cannot work).
func WithNativeCacheDir(path string) Option {
	return func(s *settings) error {
		if path == "" {
			return invalid("native cache directory cannot be empty")
		}
		s.cache = path
		return nil
	}
}

type waitSettings struct{ timeout, poll time.Duration }
type WaitOption func(*waitSettings) error

func WithWaitTimeout(d time.Duration) WaitOption {
	return func(s *waitSettings) error {
		if d < 0 {
			return invalid("wait timeout must be nonnegative")
		}
		s.timeout = d
		return nil
	}
}
func WithPollInterval(d time.Duration) WaitOption {
	return func(s *waitSettings) error {
		if d <= 0 {
			return invalid("poll interval must be positive")
		}
		s.poll = d
		return nil
	}
}
func waitOptions(options []WaitOption) (waitSettings, error) {
	s := waitSettings{300 * time.Second, 500 * time.Millisecond}
	for _, option := range options {
		if option == nil {
			return s, invalid("nil wait option")
		}
		if err := option(&s); err != nil {
			return s, err
		}
	}
	return s, nil
}
