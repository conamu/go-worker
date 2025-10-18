package worker

import (
	"context"
	"log/slog"
	"sync"
	"time"
)

type Worker struct {
	Name       string
	Wg         *sync.WaitGroup
	Ctx        context.Context
	localCtx   context.Context
	Cancel     context.CancelFunc
	Logger     *slog.Logger
	TaskFunc   func(*Worker, any)
	Interval   time.Duration
	running    bool
	InputChan  chan any
	OutputChan chan any
}

// NewWorker creates a new background Worker.
// These values will be used for logging and tracking.
// The context is used as a global shutdown signal.
// The jobTask function is the task that will be run.
// Task functions will only send and receive data with channels.
func NewWorker(ctx context.Context, name string, wg *sync.WaitGroup, jobTask func(w *Worker, msg any), logger *slog.Logger, interval time.Duration) *Worker {
	lCtx, cancelFunc := context.WithCancel(ctx)

	job := &Worker{
		Name:       name,
		Wg:         wg,
		Ctx:        ctx,
		localCtx:   lCtx,
		Cancel:     cancelFunc,
		TaskFunc:   jobTask,
		Interval:   interval,
		InputChan:  make(chan any),
		OutputChan: make(chan any),
	}

	loggerCopy := *logger
	taskLogger := &loggerCopy
	taskLogger = taskLogger.With("worker", name)
	job.Logger = taskLogger
	return job
}

// Start launches a given task as a goroutine, calling a function until the context is done.
// The wait group in the Worker can be used for the calling process to wait until execution is done.
// If a worker is configured with 0 Duration it will only trigger on input channel
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
	if w.Interval == 0 {
		t.Stop()
	}

	w.Logger.Info("started new worker")

	for {
		select {
		case <-w.localCtx.Done():
			t.Stop()
			w.Logger.Info("worker stopped")
			return
		case <-t.C:
			w.TaskFunc(w, nil)
			t.Reset(w.Interval)
		case msg := <-w.InputChan:
			w.TaskFunc(w, msg)
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
