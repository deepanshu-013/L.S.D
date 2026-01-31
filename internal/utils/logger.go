package utils

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"
)

type Logger struct {
	file   *os.File
	logger *log.Logger
	jobID  string
}

func NewLogger(jobID string) (*Logger, error) {
	// Create logs directory
	logsDir := "./logs/pipeline"
	if err := os.MkdirAll(logsDir, 0755); err != nil {
		return nil, err
	}

	// Create log file
	timestamp := time.Now().Format("20060102_150405")
	filename := filepath.Join(logsDir, fmt.Sprintf("job_%s_%s.log", jobID[:8], timestamp))

	file, err := os.OpenFile(filename, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return nil, err
	}

	logger := log.New(file, "", log.LstdFlags|log.Lmicroseconds)

	return &Logger{
		file:   file,
		logger: logger,
		jobID:  jobID,
	}, nil
}

func (l *Logger) Info(format string, v ...interface{}) {
	msg := fmt.Sprintf(format, v...)
	l.logger.Printf("[INFO] %s", msg)
	log.Printf("[Job %s] [INFO] %s", l.jobID[:8], msg)
}

func (l *Logger) Error(format string, v ...interface{}) {
	msg := fmt.Sprintf(format, v...)
	l.logger.Printf("[ERROR] %s", msg)
	log.Printf("[Job %s] [ERROR] %s", l.jobID[:8], msg)
}

func (l *Logger) Warn(format string, v ...interface{}) {
	msg := fmt.Sprintf(format, v...)
	l.logger.Printf("[WARN] %s", msg)
	log.Printf("[Job %s] [WARN] %s", l.jobID[:8], msg)
}

func (l *Logger) Debug(format string, v ...interface{}) {
	msg := fmt.Sprintf(format, v...)
	l.logger.Printf("[DEBUG] %s", msg)
	log.Printf("[Job %s] [DEBUG] %s", l.jobID[:8], msg)
}

func (l *Logger) Close() {
	if l.file != nil {
		l.file.Close()
	}
}

func (l *Logger) GetLogPath() string {
	if l.file != nil {
		return l.file.Name()
	}
	return ""
}
