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
	workerCount      = 10
	messageRejectTTL = 5 * time.Second
)

type Broker interface {
	ListenStream(
		tasksChan chan models.BrokerMessage,
		messageCount int,
	)
}

type DbClient interface {
	SaveProcessedEvent(ctx context.Context, event *models.Event) error
}

type RedisClient interface {
	SaveProcessedEvent(context.Context, *models.Event) error
	DeleteProcessedEvent(context.Context, *models.Event) error
	EventExists(senderId, payload string, createdAt int64) (bool, error)
}

type Workers interface {
	Start()
	Stop(context.Context) error
}

type WorkerStore struct {
	workerCount int
	workers     []*Worker
	tasksChan   chan models.BrokerMessage
	ctx         context.Context    // Внутренний WithCancel контекст воркера
	cancel      context.CancelFunc // Функция отмены контекста WorkerStore, для завершения воркеров
	log         *slog.Logger
	lastWorker  int
	redis       RedisClient
	db          DbClient
	broker      Broker
	wg          sync.WaitGroup
}

func NewWorkerStore(
	log *slog.Logger,
	rc RedisClient,
	dc DbClient,
	broker Broker,
) Workers {
	ctx, cancel := context.WithCancel(context.Background())

	store := &WorkerStore{
		workerCount: workerCount,
		workers:     make([]*Worker, workerCount),
		tasksChan:   make(chan models.BrokerMessage, workerCount*2),
		broker:      broker,
		log:         log,
		ctx:         ctx,
		cancel:      cancel,
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
			tasksChan:   make(chan models.BrokerMessage, 1),
			log:         ws.log,
			redisClient: ws.redis,
			dbClient:    ws.db,
			ctx:         ws.ctx,
			Status:      Available,
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
}

func (ws *WorkerStore) Stop(ctx context.Context) error {
	ws.log.Info("stopping WorkerStore")
	ws.cancel()

	close(ws.tasksChan)

	// Ожидаем завершения всех горутин
	done := make(chan struct{})
	go func() {
		ws.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		ws.log.Info("all workers and dispatcher stopped")
		return nil
	case <-ctx.Done():
		ws.log.Warn("WorkerStore shutdown timeout, forcing exit")
		return ctx.Err()
	}
}

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

func (ws *WorkerStore) dispatchToWorker(msg models.BrokerMessage) {
	ctxTimeout, cancel := context.WithTimeout(ws.ctx, messageRejectTTL)
	defer cancel()

Loop:
	// Проверяем все воркеры в цикле round-robin
	for i := 0; i < ws.workerCount; i++ {
		ws.lastWorker = (ws.lastWorker + 1) % ws.workerCount
		worker := ws.workers[ws.lastWorker]

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
			// Проверяем статус воркера
			if worker.Status == Available {
				select {
				case worker.tasksChan <- msg:
					ws.log.Debug("task dispatched to worker", "worker_id", worker.ID)
					return
				default:
					// Канал воркера занят, пробуем следующего
					continue
				}
			}
			// Если воркер не доступен (Busy или Closed), пробуем следующего
		}
	}

	goto Loop
}

func (ws *WorkerStore) startListener() {
	defer ws.wg.Done()

	ws.log.Debug("stream listener started")
	ws.broker.ListenStream(ws.tasksChan, ws.workerCount)
	ws.log.Info("stream listener stopped")
}
