package go_utils

import (
	"context"
	"time"
)

type DemoTask struct {
	Error   error
	Success bool
}

func NewDemoTask() *DemoTask {
	return &DemoTask{}
}

func (*DemoTask) GetTaskName() string {
	return "DemoTask"
}

func (DemoTask *DemoTask) SetErr(err error) {
	DemoTask.Error = err
}

func (DemoTask *DemoTask) SetSuccess(success bool) {
	DemoTask.Success = success
}

func (*DemoTask) Run(ctx context.Context) error {
	time.Sleep(2000 * time.Millisecond)
	return nil
}
