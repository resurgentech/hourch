package hourchflow

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"testing"
)

func TestCheckSendsValidateOnlyWithoutContextOrSecretValues(t *testing.T) {
	f := newFake(t)
	r, _ := contextRun(t)
	sec := r.Secret("DEPLOY_KEY", "s3cr3t-value")
	r.Job("deploy", Cmd("deploy"), Secrets(sec))
	if err := r.Check(context.Background(), f.client(t)); err != nil {
		t.Fatal(err)
	}
	if len(f.ctxReqs) != 0 {
		t.Fatal("Check uploaded a context")
	}
	if len(f.submissions) != 1 || f.validateOnly[0] != "true" {
		t.Fatalf("submissions %d, validate_only %q", len(f.submissions), f.validateOnly)
	}
	sub := f.submissions[0]
	if sub.ContextRef != nil {
		t.Fatal("Check sent a context_ref")
	}
	if sub.Secrets == nil || (*sub.Secrets)["DEPLOY_KEY"] != CheckSecretPlaceholder {
		t.Fatalf("secrets = %v", sub.Secrets)
	}
	if len(f.runs) != 0 {
		t.Fatal("a run was created")
	}
}

func TestCheckRejected(t *testing.T) {
	f := newFake(t)
	f.runStatus, f.unsatisfiable = http.StatusUnprocessableEntity, true
	r := NewRun("wf", nil)
	r.Job("build", Tags("kernel"), Cmd("true"))
	err := r.Check(context.Background(), f.client(t))
	var ae *APIError
	if !errors.As(err, &ae) || ae.Status != 422 || ae.Code != "unsatisfiable" || !strings.Contains(ae.Message, `job "build" needs tags [kernel]`) {
		t.Fatalf("err = %v", err)
	}
}

func TestCheckInvalidGraphDoesNoIO(t *testing.T) {
	f := newFake(t)
	r := NewRun("wf", nil)
	r.Job("bad", Cmd("echo", "${outputs.nope.v}"))
	var ve *ValidationError
	if err := r.Check(context.Background(), f.client(t)); !errors.As(err, &ve) {
		t.Fatalf("err = %v", err)
	}
	if f.runReqs != 0 {
		t.Fatal("request made for an invalid graph")
	}
}
