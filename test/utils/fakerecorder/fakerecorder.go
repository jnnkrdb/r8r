package fakerecorder

import (
	"fmt"
	"sync"

	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

type FakeEventRecorder struct {
	mu     sync.Mutex
	Events []string
}

func NewFakeEventRecorder() *FakeEventRecorder { return &FakeEventRecorder{} }

func (f *FakeEventRecorder) record(reason, message string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.Events = append(f.Events, fmt.Sprintf("%s: %s", reason, message))
}

func (f *FakeEventRecorder) Eventf(object, relatedObj runtime.Object, eventtype, related, reason, messageFmt string, args ...interface{}) {
	msg := fmt.Sprintf(messageFmt, args...)
	f.record(reason, msg)
}

func (f *FakeEventRecorder) PastEventf(object, relatedObj runtime.Object, timestamp v1.Time, eventtype, related, reason, messageFmt string, args ...interface{}) {
	msg := fmt.Sprintf(messageFmt, args...)
	f.record(reason, msg)
}
