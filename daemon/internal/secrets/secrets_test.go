package secrets

import (
	"bytes"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"testing"
)

func key() []byte { return bytes.Repeat([]byte{0xa7}, KeySize) }

const inst = "01JBF6Q9W2X7K3M4N5P6Q7R8S9"

func TestSealOpen(t *testing.T) {
	b, err := NewWithKey(key(), inst)
	if err != nil {
		t.Fatal(err)
	}
	vals := map[string]string{"TOKEN": "hunter2", "OTHER": "s3cr3t"}
	obj, err := b.Seal("RUN1", vals)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(obj, []byte("hunter2")) || bytes.Contains(obj, []byte("TOKEN")) {
		t.Fatal("object contains plaintext")
	}
	got, err := b.Open(inst, "RUN1", obj)
	if err != nil || len(got) != 2 || got["TOKEN"] != "hunter2" || got["OTHER"] != "s3cr3t" {
		t.Fatalf("open = %v %v", got, err)
	}
	obj2, _ := b.Seal("RUN1", vals)
	if bytes.Equal(obj[:13], obj2[:13]) {
		t.Fatal("nonce reused")
	}
	empty, _ := b.Seal("RUN3", nil)
	if m, err := b.Open(inst, "RUN3", empty); err != nil || len(m) != 0 {
		t.Fatalf("empty map = %v %v", m, err)
	}
}

func TestOpenRejectsWrongBinding(t *testing.T) {
	b, _ := NewWithKey(key(), inst)
	obj, _ := b.Seal("RUN1", map[string]string{"S": "v"})
	if _, err := b.Open(inst, "RUN2", obj); !errors.Is(err, ErrUnreadable) {
		t.Fatalf("opened under another run id: %v", err)
	}
	// Same key, other instance id: the AAD differs.
	other, _ := NewWithKey(key(), "01JBF6Q9W2X7K3M4N5P6Q7R8SA")
	if _, err := other.Open(inst, "RUN1", obj); !errors.Is(err, ErrUnreadable) {
		t.Fatalf("another instance opened it: %v", err)
	}
	obj3, _ := other.Seal("RUN1", map[string]string{"S": "v"})
	if _, err := b.Open(inst, "RUN1", obj3); !errors.Is(err, ErrUnreadable) {
		t.Fatalf("object sealed for another instance opened (AAD not bound): %v", err)
	}
	// A fresh volatile key (a restart) cannot open the old object.
	v, err := NewVolatile(inst)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := v.Open(inst, "RUN1", obj); !errors.Is(err, ErrUnreadable) {
		t.Fatalf("a new key opened the object: %v", err)
	}
	tampered := append([]byte{}, obj...)
	tampered[len(tampered)-1] ^= 1
	if _, err := b.Open(inst, "RUN1", tampered); !errors.Is(err, ErrUnreadable) {
		t.Fatal("tampered object opened")
	}
	if _, err := b.Open(inst, "RUN1", []byte{1, 2}); !errors.Is(err, ErrUnreadable) {
		t.Fatal("short object opened")
	}
}

func TestObjectKeys(t *testing.T) {
	b, _ := NewWithKey(key(), inst)
	k := b.Key("RUNX")
	if k != "secrets/"+inst+"/RUNX" {
		t.Fatalf("key = %s", k)
	}
	if i, r, ok := ParseObjectKey(k); !ok || i != inst || r != "RUNX" {
		t.Fatalf("parse = %s %s %v", i, r, ok)
	}
	for _, bad := range []string{"secrets/", "secrets/x", "secrets//r", "logs/x/y", "secrets/x/y/z"} {
		if _, _, ok := ParseObjectKey(bad); ok {
			t.Errorf("%q parsed", bad)
		}
	}
	if _, err := NewWithKey(key(), "a/b"); err == nil {
		t.Fatal("instance with / accepted")
	}
	if _, err := NewWithKey([]byte("short"), inst); err == nil {
		t.Fatal("short key accepted")
	}
}

// The key never reaches a log: a Box formats and logs as its instance id
// only, however it is printed.
func TestKeyNeverLogged(t *testing.T) {
	k := key()
	b, _ := NewWithKey(k, inst)
	var buf bytes.Buffer
	for _, h := range []slog.Handler{slog.NewTextHandler(&buf, nil), slog.NewJSONHandler(&buf, nil)} {
		l := slog.New(h)
		l.Info("box", "box", b, "any", any(b))
		l.Info("group", slog.Group("g", "box", b))
	}
	fmt.Fprintf(&buf, "%v %+v %#v %s\n", b, b, b, b)
	fmt.Fprintf(&buf, "%v %+v\n", *b, []*Box{b})
	out := buf.String()
	for _, enc := range []string{string(k), hex.EncodeToString(k), strings.ToUpper(hex.EncodeToString(k)),
		base64.StdEncoding.EncodeToString(k), base64.RawURLEncoding.EncodeToString(k), fmt.Sprint(k)} {
		if strings.Contains(out, enc) {
			t.Fatalf("key material in output (%q):\n%s", enc[:8], out)
		}
	}
	if !strings.Contains(out, "secrets.Box{instance="+inst+"}") {
		t.Fatalf("box not formatted as its instance id:\n%s", out)
	}
}

func TestRedactor(t *testing.T) {
	r := NewRedactor([]string{"abc", "abcdef", ""})
	if got := r.Redact("x abcdef y abc z"); got != "x *** y *** z" {
		t.Fatalf("redact = %q", got)
	}
	if got := NewRedactor(nil).Redact("abc"); got != "abc" {
		t.Fatalf("empty redactor changed text: %q", got)
	}
}
