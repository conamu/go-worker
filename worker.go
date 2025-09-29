package worker

import (
	"context"
	"log/slog"
	"sync"
	"time"
)

type Worker struct {
	Name          string
	Wg            *sync.WaitGroup
	Ctx           context.Context
	localCtx      context.Context
	Cancel        context.CancelFunc
	Logger        *slog.Logger
	TaskFunc      func(*Worker)
	Interval      time.Duration
	running       bool
	CommsChannels map[string]chan interface{}
}

// NewWorker creates a new background Worker.
// These values will be used for logging and tracking.
// The context is used as a global shutdown signal.
// The jobTask function is the task that will be run.
// Task functions will only send and receive data with channels.
func NewWorker(ctx context.Context, name string, wg *sync.WaitGroup, jobTask func(*Worker), logger *slog.Logger, interval time.Duration) *Worker {
	lCtx, cancelFunc := context.WithCancel(context.Background())

	job := &Worker{
		Name:          name,
		Wg:            wg,
		Ctx:           ctx,
		localCtx:      lCtx,
		Cancel:        cancelFunc,
		TaskFunc:      jobTask,
		Interval:      interval,
		CommsChannels: make(map[string]chan interface{}),
	}

	loggerCopy := *logger
	taskLogger := &loggerCopy
	taskLogger = taskLogger.With("worker", name)
	job.Logger = taskLogger
	return job
}

// Start launches a given task as a goroutine, calling a function until the context is done.
// The wait group in the Worker can be used for the calling process to wait until execution is done.
func (w *Worker) Start() {
	if w == nil {
		return
	}
	if w.running {
		return
	}
	w.running = true
	w.Wg.Add(1)
	defer w.Wg.Done()

	t := time.NewTimer(0)

	w.Logger.Info("started new worker")

	for {
		select {
		case <-w.localCtx.Done():
			t.Stop()
			w.Logger.Info("worker stopped")
			return
		case <-w.Ctx.Done():
			t.Stop()
			w.Logger.Info("worker stopped")
			return
		case <-t.C:
			w.TaskFunc(w)
			t.Reset(w.Interval)
		}
	}
}

func (w *Worker) Stop() {
	if w == nil {
		return
	}
	if w.running {
		w.Logger.Info("stopping worker")
		w.Cancel()
	}
}
