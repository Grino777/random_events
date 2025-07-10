package worker

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/Grino777/random_events/internal/domain/models"
	"github.com/Grino777/random_events/internal/lib/logger"
)

const (
	workerCount      = 5
	messageRejectTTL = 5 * time.Second
)

type Broker interface {
	ListenStream(
		ctx context.Context,
		tasksChan chan models.EventMessage,
		messageCount int,
	)
}

type DbClient interface {
	SaveProcessedEvent(ctx context.Context, event *models.Event) error
}

type RedisClient interface {
	SaveProcessedEvent(context.Context, *models.Event) error
	EventExists(senderId, payload string, createdAt int64) (bool, error)
}

type Workers interface {
	Start()
}

type WorkerStore struct {
	workerCount int
	workers     []*Worker
	tasksChan   chan models.EventMessage
	ctx         context.Context
	log         *slog.Logger
	lastWorker  int
	redis       RedisClient
	db          DbClient
	broker      Broker
	wg          sync.WaitGroup
}

func NewWorkerStore(
	ctx context.Context,
	log *slog.Logger,
	rc RedisClient,
	dc DbClient,
	broker Broker,
) Workers {
	store := &WorkerStore{
		workerCount: workerCount,
		workers:     make([]*Worker, workerCount),
		tasksChan:   make(chan models.EventMessage, workerCount*2),
		broker:      broker,
		log:         log,
		ctx:         ctx,
		lastWorker:  -1,
		redis:       rc,
		db:          dc,
	}
	return store
}

func (ws *WorkerStore) Start() {
	for i := range ws.workerCount {
		worker := &Worker{
			ID:          i,
			tasksChan:   make(chan models.EventMessage, 1),
			log:         ws.log,
			redisClient: ws.redis,
			dbClient:    ws.db,
			ctx:         ws.ctx,
		}

		ws.workers[i] = worker
		ws.wg.Add(1)
		go func(w *Worker) {
			defer ws.wg.Done()
			worker.Start()
		}(worker)
	}

	ws.wg.Add(1)
	go ws.dispatchTasks()

	ws.wg.Add(1)
	go ws.startListener()

	<-ws.ctx.Done()
	if err := ws.Stop(); err != nil {
		ws.log.Error("failed to stop workers", logger.Error(err))
	}
	// go func() {
	// 	<-ws.ctx.Done()
	// 	if err := ws.Stop(); err != nil {
	// 		ws.log.Error("failed to stop workers", logger.Error(err))
	// 	}
	// }()
}

func (ws *WorkerStore) Stop() error {
	ws.log.Info("stopping WorkerStore")

	// Создаем контекст с таймаутом для завершения
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Ожидаем завершения всех горутин
	done := make(chan struct{})
	go func() {
		ws.wg.Wait()
		close(done)
	}()

	ws.log.Debug("all worker gorutines stopped")

	// Закрываем канал tasksChan, чтобы диспетчер задач прекратил работу
	close(ws.tasksChan)

	// Закрываем каналы tasksChan для всех воркеров
	for _, worker := range ws.workers {
		close(worker.tasksChan)
	}

	// Ожидаем завершения или таймаута
	select {
	case <-done:
		ws.log.Info("all workers and dispatcher stopped")
		return nil
	case <-ctx.Done():
		ws.log.Warn("WorkerStore shutdown timeout, forcing exit")
		return ctx.Err()
	}
}

// func (ws *WorkerStore) Stop() error {
// 	ws.log.Info("stopping tasks dispatch in Worker Store")

// 	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
// 	defer cancel()

// 	// Ожидаем завершения всех воркеров и диспетчера
// 	done := make(chan struct{})
// 	go func() {
// 		ws.wg.Wait()
// 		close(done)
// 	}()

// 	// Ожидаем завершения или таймаута
// 	select {
// 	case <-done:
// 		ws.log.Info("all workers and dispatcher stopped")
// 		close(ws.tasksChan)
// 		// Закрываем каналы Tasks для всех воркеров
// 		for _, worker := range ws.workers {
// 			close(worker.tasksChan)
// 		}
// 		return nil
// 	case <-ctx.Done():
// 		ws.log.Warn("WorkerStore shutdown timeout, forcing exit")
// 		close(ws.tasksChan)
// 		for _, worker := range ws.workers {
// 			close(worker.tasksChan)
// 		}
// 		return ctx.Err()
// 	}
// }

func (ws *WorkerStore) dispatchTasks() {
	defer ws.wg.Done()

	ws.log.Info("task dispatcher started")

	for {
		select {
		case <-ws.ctx.Done():
			ws.log.Info("task dispatcher stopped")
			return
		case task, ok := <-ws.tasksChan:
			if !ok {
				ws.log.Info("task dispatcher stopped due to closed channel")
				return
			}
			ws.dispatchToWorker(task)
		}
	}
}

func (ws *WorkerStore) dispatchToWorker(msg models.EventMessage) {
	ctxTimeout, cancel := context.WithTimeout(ws.ctx, messageRejectTTL)
	defer cancel()

	for {
		select {
		case <-ws.ctx.Done():
			ws.log.Debug("task dispatcher stopped while dispatching task")
			if err := msg.Reject(); err != nil {
				ws.log.Error("failed to reject broker message", logger.Error(err))
			}
			msg.Close()
			return
		case <-ctxTimeout.Done():
			ws.log.Debug("task dispatch timed out")
			if err := msg.Reject(); err != nil {
				ws.log.Error("failed to reject broker message", logger.Error(err))
			}
			msg.Close()
			return
		default:
			// Round-robin выбор воркера
			ws.lastWorker = (ws.lastWorker + 1) % ws.workerCount
			worker := ws.workers[ws.lastWorker]

			select {
			case worker.tasksChan <- msg:
				ws.log.Debug("task dispatched to worker", "worker_id", worker.ID)
				return
			default:
				for i := 0; i < ws.workerCount; i++ {
					workerIndex := (ws.lastWorker + i) % ws.workerCount
					worker = ws.workers[workerIndex]
					worker.tasksChan <- msg
					ws.lastWorker = workerIndex
					ws.log.Debug("task dispatched to worker", "worker_id", worker.ID)
					return
				}
			}
		}
	}
}

func (ws *WorkerStore) startListener() {
	defer ws.wg.Done()

	ws.log.Debug("stream listener started")
	ws.broker.ListenStream(ws.ctx, ws.tasksChan, ws.workerCount)
	ws.log.Info("stream listener stopped")
}
