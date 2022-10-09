package go_utils

import (
	"context"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

func TestDemoTask(t *testing.T) {
	demoTask := NewDemoTask()
	ctx := context.Background()
	var taskList []Task
	taskList = append(taskList, demoTask)
	Convey("parallel task", t, func() {
		tasks := NewParallelTaskExecWithTimeout(ctx, 2000, taskList...)
		taskErr := tasks.Exec()
		So(taskErr, ShouldNotBeNil)
	})
}
