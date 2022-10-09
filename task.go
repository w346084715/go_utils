package go_utils

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"time"
)

const (
	BUF_SIZE    = (64 << 10)
	RPC_TIMEOUT = 1000000000 //nanoseconds
)

type Task interface {
	Run(ctx context.Context) error

	GetTaskName() string

	SetErr(error)

	SetSuccess(bool)
}

type ParallelTaskExec struct {
	Context     context.Context
	Tasks       []Task
	SuccessInfo map[string]bool
	Timeout     time.Duration
}

func NewParallelTaskExec(context context.Context, tasks ...Task) *ParallelTaskExec {
	parallelTaskExec := &ParallelTaskExec{
		Context:     context,
		Tasks:       tasks,
		SuccessInfo: make(map[string]bool),
		Timeout:     RPC_TIMEOUT,
	}
	return parallelTaskExec
}

func NewParallelTaskExecWithTimeout(context context.Context, timeout int, tasks ...Task) *ParallelTaskExec {
	return NewParallelTaskExec(context, tasks...).SetParallelTimeout(timeout)
}

func (parallelTaskExec *ParallelTaskExec) SetParallelTimeout(timeout int) *ParallelTaskExec {
	parallelTaskExec.Timeout = time.Duration(timeout) * time.Millisecond
	return parallelTaskExec
}

func (parallelTaskExec *ParallelTaskExec) AppendTask(task Task) {
	if task != nil {
		parallelTaskExec.Tasks = append(parallelTaskExec.Tasks, task)
	}
}

func (parallelTaskExec *ParallelTaskExec) GetTaskNum() int {
	return len(parallelTaskExec.Tasks)
}

func (parallelTaskExec *ParallelTaskExec) GetTaskLogName(index int) string {
	// add index to task name
	if index >= parallelTaskExec.GetTaskNum() {
		return fmt.Sprintf("%d_unknow", index)
	}
	taskName := parallelTaskExec.Tasks[index].GetTaskName()
	taskLogName := fmt.Sprintf("%d_%s", index, taskName)
	return taskLogName
}

// 并行执行所有任务，等待所有任务执行结束后返回，如果超时， 返回err
func (parallelTaskExec *ParallelTaskExec) Exec() error {
	defer func() {
		if e := recover(); e != nil {
			buf := make([]byte, BUF_SIZE)
			buf = buf[:runtime.Stack(buf, false)]
		}
	}()
	// init success info for log
	for index := 0; index < parallelTaskExec.GetTaskNum(); index++ {
		taskSuccName := parallelTaskExec.GetTaskLogName(index)
		parallelTaskExec.SuccessInfo[taskSuccName] = false
	}

	st := time.Now()
	taskNum := len(parallelTaskExec.Tasks)
	indexChan := make(chan int, taskNum)
	c, cancelTask := context.WithCancel(parallelTaskExec.Context)
	for i, task := range parallelTaskExec.Tasks {
		go func(ctx context.Context, t Task, index int) {
			//startTime := time.Now()
			defer func() {
				if e := recover(); e != nil {
					buf := make([]byte, BUF_SIZE)
					buf = buf[:runtime.Stack(buf, false)]
					//tagkv := map[string]string{"task_name": t.GetTaskName()}
				}
			}()
			if err := t.Run(ctx); err != nil {
				t.SetSuccess(false)
				t.SetErr(err)
				//tagkv := map[string]string{"task_name": t.GetTaskName()}
			} else {
				t.SetSuccess(true)
			}
			indexChan <- index
			//usedTime := time.Since(startTime).Nanoseconds() / 1000
			//tagkv := map[string]string{"task_name": t.GetTaskName()}
		}(c, task, i)
	}
	succeccCount := 0
	timeoutDuration := parallelTaskExec.Timeout
	if timeoutDuration <= 0 {
		timeoutDuration = RPC_TIMEOUT
	}
	timeout := time.After(timeoutDuration)
	for succeccCount < taskNum {
		select {
		case index := <-indexChan:
			taskSuccName := parallelTaskExec.GetTaskLogName(index)
			parallelTaskExec.SuccessInfo[taskSuccName] = true
			succeccCount = succeccCount + 1
			durationTime := time.Since(st).Nanoseconds() / 1000
			if durationTime >= 50000 { // 50ms

			}
		case <-timeout:
			cancelTask()
			return errors.New("task timeout")
		}
	}
	return nil
}
