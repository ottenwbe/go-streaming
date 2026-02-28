package log

import "log"

// Logger defines the interface for logging within the library.
// Consumers should implement this interface to capture library logs.
type Logger interface {
	Debug(args ...interface{})
	Info(args ...interface{})
	Warn(args ...interface{})
	Error(args ...interface{})

	Debugf(format string, args ...interface{})
	Infof(format string, args ...interface{})
	Warnf(format string, args ...interface{})
	Errorf(format string, args ...interface{})
}

// globalLogger is the instance used by the library. Defaults to no-op.
var globalLogger Logger = &noopLogger{}

// SetLogger allows the consumer to inject a custom logger.
func SetLogger(l Logger) {
	if l != nil {
		globalLogger = l
	}
}

// Loggers for internal use

func Debug(args ...interface{}) { globalLogger.Debug(args...) }
func Info(args ...interface{})  { globalLogger.Info(args...) }
func Warn(args ...interface{})  { globalLogger.Warn(args...) }
func Error(args ...interface{}) { globalLogger.Error(args...) }

func Debugf(format string, args ...interface{}) { globalLogger.Debugf(format, args...) }
func Infof(format string, args ...interface{})  { globalLogger.Infof(format, args...) }
func Warnf(format string, args ...interface{})  { globalLogger.Warnf(format, args...) }
func Errorf(format string, args ...interface{}) { globalLogger.Errorf(format, args...) }

// noopLogger is the default logger that discards all messages.
type noopLogger struct{}

func (n *noopLogger) Debug(args ...interface{}) {}
func (n *noopLogger) Info(args ...interface{})  {}
func (n *noopLogger) Warn(args ...interface{})  {}
func (n *noopLogger) Error(args ...interface{}) {}

func (n *noopLogger) Debugf(format string, args ...interface{}) {}
func (n *noopLogger) Infof(format string, args ...interface{})  {}
func (n *noopLogger) Warnf(format string, args ...interface{})  {}
func (n *noopLogger) Errorf(format string, args ...interface{}) {}

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

func (s *StdLogger) Debug(args ...interface{})                 { s.output("DEBUG", args...) }
func (s *StdLogger) Info(args ...interface{})                  { s.output("INFO", args...) }
func (s *StdLogger) Warn(args ...interface{})                  { s.output("WARN", args...) }
func (s *StdLogger) Error(args ...interface{})                 { s.output("ERROR", args...) }
func (s *StdLogger) Debugf(format string, args ...interface{}) { s.outputf("DEBUG", format, args...) }
func (s *StdLogger) Infof(format string, args ...interface{})  { s.outputf("INFO", format, args...) }
func (s *StdLogger) Warnf(format string, args ...interface{})  { s.outputf("WARN", format, args...) }
func (s *StdLogger) Errorf(format string, args ...interface{}) { s.outputf("ERROR", format, args...) }

func (s *StdLogger) output(level string, args ...interface{}) {
	s.l.Print(append([]interface{}{level + ": "}, args...)...)
}

func (s *StdLogger) outputf(level string, format string, args ...interface{}) {
	s.l.Printf(level+": "+format, args...)
}
