package raft

import (
	"sync"
	"sync/atomic"
)

type AppendEntryHandler struct {
	waitGroup *sync.WaitGroup
	threshold int
	success   int
	fail      int
	ok        int32
}

func (h *AppendEntryHandler) onResp(ok bool) {
	if h.waitGroup == nil || atomic.LoadInt32(&h.ok) > 0 {
		return
	}

	if ok {
		h.success += 1
	} else {
		h.fail += 1
	}
	if h.success >= h.threshold {
		atomic.StoreInt32(&h.ok, 1)
		h.waitGroup.Done()
	} else if h.fail >= h.threshold {
		atomic.StoreInt32(&h.ok, 2)
		h.waitGroup.Done()
	}
}

func (h *AppendEntryHandler) isOk() bool {
	return atomic.LoadInt32(&h.ok) == 1
}
