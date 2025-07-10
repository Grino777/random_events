package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/Grino777/random_events/internal/app"
	"github.com/Grino777/random_events/internal/lib/logger"
)

func main() {
	log := logger.NewLogger(os.Stdout, slog.LevelDebug)
	ctx, cancel := context.WithCancel(context.Background())

	// Graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	errChan := make(chan error, 2)

	app := app.NewApp(ctx, log)

	go func() {
		if err := app.Start(errChan); err != nil {
			log.Error("failed to start app", logger.Error(err))
			return
		}
	}()

	// Ожидаем завершения всех горутин
	done := make(chan struct{})
	go func() {
		app.Wg.Wait()
		close(done)
	}()

	select {
	case <-sigChan:
		log.Info("received shutdown signal")
		cancel()
	case err := <-errChan:
		log.Error("application stopped with error", logger.Error(err))
		cancel()
	}

	log.Info("Application shutdown")
}
