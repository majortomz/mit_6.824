package mr

import (
	"runtime/debug"
	"testing"
)

func TestHandleRequest(t *testing.T) {
	coordinator := MakeCoordinator([]string{"test_resources/test_input"}, 2)

	req := WorkerRequest{
		WorkerId: "test-1",
		ReqType:  IDLE,
	}
	resp := WorkResponse{}
	coordinator.RequestWork(&req, &resp)
	assertEquals(t, req.WorkerId, coordinator.mapWorkState[0].workerId)
	assertEquals(t, MAP, resp.WorkType)

	req = WorkerRequest{
		WorkerId: "test-1",
		ReqType:  IDLE,
	}
	resp = WorkResponse{}
	coordinator.RequestWork(&req, &resp)
	assertEquals(t, HOLD, resp.WorkType)

	req = WorkerRequest{
		WorkerId: "test-1",
		ReqType:  FINISH,
		WorkType: MAP,
		WorkId:   0,
		FilePath: []string{"output-1", ""},
	}
	resp = WorkResponse{}
	coordinator.RequestWork(&req, &resp)
	assertEquals(t, 1, coordinator.mapWorkFinishCount)
	assertEquals(t, CANCEL, resp.WorkType)
	assertEquals(t, "output-1", coordinator.reduceWorkState[0].inputPath[0])

	req = WorkerRequest{
		WorkerId: "test-1",
		ReqType:  IDLE,
	}
	resp = WorkResponse{}
	coordinator.RequestWork(&req, &resp)
	assertEquals(t, REDUCE, resp.WorkType)

	req = WorkerRequest{
		WorkerId: "test-1",
		ReqType:  FINISH,
		WorkType: REDUCE,
		WorkId:   0,
		FilePath: []string{"output-1", ""},
	}
	resp = WorkResponse{}
	coordinator.RequestWork(&req, &resp)
	assertEquals(t, 1, coordinator.reduceWorkFinishCount)
	assertEquals(t, CANCEL, resp.WorkType)

}

func assertEquals(t *testing.T, expected interface{}, real interface{}) {
	if expected != real {
		debug.PrintStack()
		t.Errorf("expected:%v real:%v", expected, real)
	}
}
