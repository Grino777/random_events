package worker

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/Grino777/random_events/internal/domain/models"
	"github.com/Grino777/random_events/internal/lib/logger"
)

const (
	opWorker = "consumer.store."
)

// worker statuses
const (
	Busy      = "busy"
	Available = "available"
	Closed    = "closed"
)

type Worker struct {
	ID          int
	tasksChan   chan models.BrokerMessage
	log         *slog.Logger
	redisClient RedisClient
	dbClient    DbClient
	ctx         context.Context
	Status      string
}

func (w *Worker) Start() {
	w.log.Info("worker started", "worker_id", w.ID)
	defer func() {
		defer close(w.tasksChan)
		w.Status = Closed
	}()

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

func (w *Worker) processMessage(msg models.BrokerMessage) error {
	const op = opWorker + "processMessage"

	defer func() {
		msg.Close()
		w.Status = Available
	}()

	w.Status = Busy

	select {
	case <-w.ctx.Done():
		return fmt.Errorf("%s: task processing interrupted due to context cancellation, worker_id: %d", op, w.ID)
	default:
		event, err := models.EventFromJSON(msg.Data())
		if err != nil {
			return w.rejectMessage(op, msg, err)
		}

		if err := w.checkExistsEvent(event, msg); err != nil {
			return err
		}

		// Имитация времени выполнения события с учетом контекста
		sleepTime := time.Duration(event.ExecTime) * time.Second
		timer := time.NewTimer(sleepTime)
		// Останавливаем таймер, чтобы избежать утечек
		defer timer.Stop()

		select {
		case <-w.ctx.Done():
			if err := msg.Reject(); err != nil {
				w.log.Error("failed to reject broker message", logger.Error(err))
				return fmt.Errorf("%s: task processing interrupted due to context cancellation, worker_id: %d", op, w.ID)
			}
			return w.ctx.Err()
		case <-timer.C:
			// Таймер сработал, продолжаем обработку
		}

		if err := w.saveMessage(event, msg); err != nil {
			return w.rejectMessage(op, msg, err)
		}
		return nil
	}
}

func (w *Worker) rejectMessage(op string, msg models.BrokerMessage, err error) error {
	if rejectErr := msg.Reject(); rejectErr != nil {
		return fmt.Errorf("%s: failed to reject broker message %v", op, rejectErr)
	}
	return fmt.Errorf("%s: %v", op, err)
}

// Проверка на уже обработанный event
func (w *Worker) checkExistsEvent(event *models.Event, msg models.BrokerMessage) error {
	const op = opWorker + "checkExistsEvent"

	exist, err := w.redisClient.EventExists(event.SenderId, event.Payload, event.CreatedAt)
	if err != nil {
		return w.rejectMessage(op, msg, err)
	}
	if exist {
		w.log.Debug("event already exists")
		if err := msg.Accept(); err != nil {
			return fmt.Errorf("%s: failed to accept broker message %v", op, err)
		}
	}
	return nil
}

func (w *Worker) saveMessage(event *models.Event, msg models.BrokerMessage) error {
	const op = opWorker + "saveMessage"

	select {
	case <-w.ctx.Done():
		return fmt.Errorf("cancelled save message due to context concellation")
	default:
		if err := w.redisClient.SaveProcessedEvent(w.ctx, event); err != nil {
			return w.rejectMessage(op, msg, err)
		}

		// Попытка сохранить в PostgreSQL
		if err := w.dbClient.SaveProcessedEvent(w.ctx, event); err != nil {
			if deleteErr := w.redisClient.DeleteProcessedEvent(w.ctx, event); deleteErr != nil {
				w.log.Error("failed to rollback Redis save", logger.Error(deleteErr))
				// Не возвращаем deleteErr, чтобы основная ошибка от PostgreSQL осталась приоритетной
			}
			return w.rejectMessage(op, msg, err)
		}
		if err := msg.Accept(); err != nil {
			w.log.Error("failed to accept broker message but massage saved", "worker_id", w.ID, logger.Error(err))
			return fmt.Errorf("%s: %v", op, err)
		}
		w.log.Debug("message accept and saved", "worker_id", w.ID)
	}
	return nil
}
