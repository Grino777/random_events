package logger

import (
	"io"
	"log/slog"
)

func NewLogger(out io.Writer,
	level slog.Level,
) *slog.Logger {
	options := &slog.HandlerOptions{Level: level} // Используем переданный level
	handler := slog.NewJSONHandler(out, options)
	return slog.New(handler)
}

func AddAttrs(log *slog.Logger, key, value string) *slog.Logger {
	l := log.With(slog.String(key, value))
	return l
}
