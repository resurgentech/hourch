package dispatch

import (
	"testing"

	proto "github.com/resurgentech/hourch/protocol"

	"github.com/resurgentech/hourch/daemon/internal/store"
)

// H2b: the worker resends outputs after a reconnect when its result was not
// sent yet, and renumbers buffered log chunks from welcome.last_log_seq+1.
// Nothing may precede welcome on a new connection.
func TestReattachResendIsIdempotent(t *testing.T) {
	e := newEnv(t)
	r := e.submit(nil, job("j"))
	s, c := e.connect("w", nil, nil, "")
	a := lastAssign(t, c)
	e.send(s, &proto.Accept{JobID: a.Job.ID})
	e.send(s, &proto.Log{JobID: a.Job.ID, Stream: "stdout", Seq: 1, Data: "a"})
	e.send(s, &proto.Output{JobID: a.Job.ID, Key: "v", Value: "1"})
	e.eng.Disconnect(s)
	s2, c2 := e.connect("w", nil, nil, a.Job.ID)
	if _, ok := c2.all()[0].(*proto.Welcome); !ok {
		t.Fatalf("first message %T", c2.all()[0])
	}
	if w := ofType[*proto.Welcome](c2); len(w) != 1 || w[0].LastLogSeq != 1 {
		t.Fatalf("welcome = %+v", w)
	}
	e.send(s2, &proto.Log{JobID: a.Job.ID, Stream: "stdout", Seq: 2, Data: "b"})
	e.send(s2, &proto.Output{JobID: a.Job.ID, Key: "v", Value: "1"})
	e.send(s2, &proto.Output{JobID: a.Job.ID, Key: "v", Value: "2"})
	if o, _ := e.st.Outputs(a.Job.ID); len(o) != 1 || o["v"] != "2" {
		t.Fatalf("outputs = %v", o)
	}
	if j := e.job(r.ID, "j"); j.LogSeq != 2 || j.LogSize != 2 {
		t.Fatalf("log pos = %d/%d", j.LogSeq, j.LogSize)
	}
	e.send(s2, &proto.Result{JobID: a.Job.ID, ExitCode: 143, Error: "cancelled: killed"})
	if j := e.job(r.ID, "j"); j.Status != store.JobFailed || j.Error != "cancelled: killed" || *j.ExitCode != 143 {
		t.Fatalf("job = %+v", j)
	}
}
