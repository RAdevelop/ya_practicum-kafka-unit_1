package logger

import "log"

func Info(msg string) {
	log.Printf("ℹ️ %s", msg)
}

func Error(msg string, err error) {
	log.Printf("❌ %s: %v", msg, err)
}

func Success(msg string) {
	log.Printf("✅ %s", msg)
}
