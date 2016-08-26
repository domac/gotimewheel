package timewheel

import (
	"testing"
	"time"
)

type MyTask struct {
}

func (self MyTask) Expire() {
	println("I am Expired")
}

//测试TimeWheel的运行
func TestGoTimeWheel(t *testing.T) {
	tw := NewTimeWheel(100*time.Millisecond, 12)
	mytask := MyTask{}
	mytask2 := MyTask{}

	tw.Start()
	_, c1 := tw.AddTask(3*time.Second, mytask)
	_, c2 := tw.AddTask(6*time.Second, mytask2)

	<-c1
	<-c2
	tw.Stop()
}

type MyBenchTask struct {
}

func (self MyBenchTask) Expire() {
}

func BenchmarkTimeWheel(t *testing.B) {
	tw := NewTimeWheel(100*time.Millisecond, 12)
	for i := 0; i < t.N; i++ {
		taskId, _ := tw.AddTask(3*time.Second, MyBenchTask{})
		tw.RemoveTask(taskId)
	}
}
