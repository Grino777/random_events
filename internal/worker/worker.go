package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/Grino777/random_events/internal/domain/models"
	"github.com/Grino777/random_events/internal/lib/logger"
)

const (
	opWorker = "consumer.store."
)

type Worker struct {
	ID          int
	tasksChan   chan models.EventMessage
	log         *slog.Logger
	redisClient RedisClient
	dbClient    DbClient
	ctx         context.Context
}

func (w *Worker) Start() {
	w.log.Info("worker started", "worker_id", w.ID)
	for {
		select {
		case <-w.ctx.Done():
			w.log.Info("worker stopped", "worker_id", w.ID)
			return
		case task, ok := <-w.tasksChan:
			if !ok {
				w.log.Info("worker stopped due to closed channel", "worker_id", w.ID)
				return
			}
			if err := w.processMessage(task); err != nil {
				w.log.Error("failed to process message", "worker_id", w.ID, logger.Error(err))
			}
		}
	}
}

func (w *Worker) processMessage(msg models.EventMessage) error {
	const op = opWorker + "processMessage"

	defer msg.Close()

	event := &models.Event{}
	if err := json.Unmarshal(msg.Data(), event); err != nil {
		w.log.Error("failed to unmarshal event", "worker_id", w.ID, logger.Error(err))
		return fmt.Errorf("%s: %v", op, err)
	}

	// Проверка на уже обработанный event
	exist, err := w.redisClient.EventExists(event.SenderId, event.Payload, event.CreatedAt)
	if err != nil {
		if err := msg.Reject(); err != nil {
			w.log.Error("failed to reject broker message", logger.Error(err))
			return fmt.Errorf("%s: %v", op, err)
		}
		return fmt.Errorf("%s: %v", op, err)
	}
	if exist {
		w.log.Debug("event already exists")
		if err := msg.Accept(); err != nil {
			w.log.Error("failed to accept broker message", logger.Error(err))
			return fmt.Errorf("%s: %v", op, err)
		}
		return nil
	}

	// Имитация времени выполнения события с учетом контекста
	sleepTime := time.Duration(event.ExecTime) * time.Second
	timer := time.NewTimer(sleepTime)
	defer timer.Stop() // Останавливаем таймер, чтобы избежать утечек
	select {
	case <-w.ctx.Done():
		w.log.Info("task processing interrupted due to context cancellation", "worker_id", w.ID)
		if err := msg.Reject(); err != nil {
			w.log.Error("failed to reject broker message", logger.Error(err))
			return fmt.Errorf("%s: %v", op, err)
		}
		return w.ctx.Err()
	case <-timer.C:
		// Таймер сработал, продолжаем обработку
	}

	if err := w.redisClient.SaveProcessedEvent(w.ctx, event); err != nil {
		if err := msg.Reject(); err != nil {
			w.log.Error("failed to reject broker message", logger.Error(err))
			return fmt.Errorf("%s: %v", op, err)
		}
		return err
	}

	if err := w.dbClient.SaveProcessedEvent(w.ctx, event); err != nil {
		if err := msg.Reject(); err != nil {
			w.log.Error("failed to reject broker message", logger.Error(err))
			return fmt.Errorf("%s: %v", op, err)
		}
		return err
	}

	if err := msg.Accept(); err != nil {
		w.log.Error("failed to accept broker message", logger.Error(err))
		return fmt.Errorf("%s: %v", op, err)
	}
	w.log.Debug("message accept and save", "worker_id", w.ID)

	return nil
}

// func (w *Worker) processMessage(msg models.EventMessage) error {
// 	const op = opWorker + "processMessage"

// 	defer msg.Close()

// 	event := &models.Event{}
// 	if err := json.Unmarshal(msg.Data(), event); err != nil {
// 		w.log.Error("failed to unmarshal event", "worker_id", w.ID, "error", err)
// 		return fmt.Errorf("%s: %v", op, err)
// 	}

// 	// Проверка на уже обработанный event
// 	exist, err := w.redisClient.EventExists(event.SenderId, event.Payload, event.CreatedAt)
// 	if err != nil {
// 		if err := msg.Reject(); err != nil {
// 			w.log.Error("failed to reject broker message")
// 			return fmt.Errorf("%s: %v", op, err)
// 		}
// 		return fmt.Errorf("%s: %v", op, err)
// 	}
// 	if exist {
// 		w.log.Debug("event alreay exist")
// 		if err := msg.Accept(); err != nil {
// 			w.log.Error("failed to accept broker message")
// 			return fmt.Errorf("%s: %v", op, err)
// 		}
// 		return nil
// 	}

// 	// Имитация времени выполнения события
// 	sleepTime := time.Duration(event.ExecTime) * time.Second
// 	time.Sleep(sleepTime)

// 	ctx := context.TODO()
// 	if err := w.redisClient.SaveProcessedEvent(ctx, event); err != nil {
// 		if err := msg.Reject(); err != nil {
// 			w.log.Error("failed to reject broker message")
// 			return fmt.Errorf("%s: %v", op, err)
// 		}
// 		return err
// 	}

// 	if err := w.dbClient.SaveProcessedEvent(event); err != nil {
// 		if err := msg.Reject(); err != nil {
// 			w.log.Error("failed to reject broker message")
// 			return fmt.Errorf("%s: %v", op, err)
// 		}
// 		return err
// 	}

// 	if err := msg.Accept(); err != nil {
// 		w.log.Error("failed to accept broker message")
// 	}
// 	w.log.Debug("message accept and save", "worker_id", w.ID)

// 	return nil
// }
