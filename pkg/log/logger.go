package log

import "log"

// Logger defines the interface for logging within the library.
// Consumers should implement this interface to capture library logs.
type Logger interface {
	Error(args ...any)
	Errorf(format string, args ...any)
}

// globalLogger is the instance used by the library. Defaults to no-op.
var globalLogger Logger = &noLog{}

// SetLogger allows the consumer to inject a custom logger, e.g., it allows to set loggers like
func SetLogger(l Logger) {
	if l != nil {
		globalLogger = l
	}
}

// Loggers for internal use - by default compatible with a lot of logging libraries

func Error(args ...any)                 { globalLogger.Error(args...) }
func Errorf(format string, args ...any) { globalLogger.Errorf(format, args...) }

// noLog is the default logger that discards all messages.
type noLog struct{}

func (n *noLog) Error(args ...any)                 {}
func (n *noLog) Errorf(format string, args ...any) {}

// StdLogger is a wrapper for the standard library logger to satisfy the Logger interface.
type StdLogger struct {
	l *log.Logger
}

// NewStdLogger creates a Logger backed by the standard library logger.
// If l is nil, it uses the default standard logger.
func NewStdLogger(l *log.Logger) Logger {
	if l == nil {
		l = log.Default()
	}
	return &StdLogger{l: l}
}

func (s *StdLogger) Error(args ...any)                 { s.output("ERROR", args...) }
func (s *StdLogger) Errorf(format string, args ...any) { s.outputf("ERROR", format, args...) }

func (s *StdLogger) output(level string, args ...any) {
	s.l.Print(append([]interface{}{level + ": "}, args...)...)
}

func (s *StdLogger) outputf(level string, format string, args ...any) {
	s.l.Printf(level+": "+format, args...)
}
