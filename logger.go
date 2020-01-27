package sqsq

import (
	"fmt"
	"log"
	"os"
)

// Logger interface for sqsq
type Logger interface {
	Debugf(format string, v ...interface{})
	Printf(format string, v ...interface{})
	Errorf(format string, v ...interface{})
}

type defaultLogger struct {
	debug  bool
	stdout *log.Logger
	stderr *log.Logger
}

// NewDefaultLogger returns a default impl logger
func NewDefaultLogger(debug bool) Logger {
	return &defaultLogger{
		debug:  debug,
		stdout: log.New(os.Stdout, "[sqsq]", 0),
		stderr: log.New(os.Stderr, "[sqsq]", 0),
	}
}

func (l *defaultLogger) Debugf(format string, v ...interface{}) {
	if l.debug {
		l.stdout.Printf(fmt.Sprintf("[DEBUG] $%s", format), v...)
	}
}

func (l *defaultLogger) Printf(format string, v ...interface{}) {
	l.stdout.Printf(format, v...)
}

func (l *defaultLogger) Errorf(format string, v ...interface{}) {
	l.stderr.Printf(fmt.Sprintf("[ERROR] $%s", format), v...)
}
