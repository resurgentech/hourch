package e2e

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	flow "github.com/resurgentech/hourch/flow"
)

// The jobs' commands are this test binary re-executed in helper mode:
//
//	<test binary> e2e-helper <mode> [args...]
//
// TestMain dispatches before any test runs. E2E_DIR (job env) is a
// directory shared with the test for markers.
const helperArg = "e2e-helper"

func helperMain(args []string) int {
	if len(args) == 0 {
		fmt.Fprintln(os.Stderr, "helper: no mode")
		return 2
	}
	if err := helper(args[0], args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "helper:", err)
		return 1
	}
	return 0
}

func shared(name string) string { return filepath.Join(os.Getenv("E2E_DIR"), name) }

func touch(name, content string) error { return os.WriteFile(shared(name), []byte(content), 0o644) }

func waitFile(name string, d time.Duration) error {
	deadline := time.Now().Add(d)
	for {
		if _, err := os.Stat(shared(name)); err == nil {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("%s did not appear", name)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func readFile(p string) (string, error) {
	b, err := os.ReadFile(p)
	return string(b), err
}

func helper(mode string, args []string) error {
	switch mode {
	case "prep":
		// Reads the context, writes a file and a directory artifact and
		// two outputs.
		hello, err := readFile("hello.txt")
		if err != nil {
			return err
		}
		if _, err := os.Stat("ignored.txt"); err == nil {
			return errors.New("ignored.txt is in the context (.gitignore not honoured)")
		}
		g := strings.TrimSpace(hello)
		for p, c := range map[string]string{"out/file.txt": "file:" + g, "out/dir/a.txt": "a", "out/dir/sub/b.txt": "b"} {
			if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
				return err
			}
			if err := os.WriteFile(p, []byte(c), 0o644); err != nil {
				return err
			}
		}
		fmt.Println("prep: wrote artifacts")
		return flow.WriteOutputs(map[string]string{"greeting": g, "number": "42"})
	case "consume":
		// argv and env carry ${outputs.prep.greeting}; inputs from the
		// other worker.
		if len(args) != 1 || args[0] != "hello-ctx" {
			return fmt.Errorf("argv greeting = %q", args)
		}
		if g := os.Getenv("GREETING"); g != "hello-ctx" {
			return fmt.Errorf("env GREETING = %q", g)
		}
		for p, want := range map[string]string{"inputs/prep/out/file.txt": "file:hello-ctx", "indir/a.txt": "a", "indir/sub/b.txt": "b"} {
			got, err := readFile(p)
			if err != nil || got != want {
				return fmt.Errorf("input %s = %q, %v; want %q", p, got, err, want)
			}
		}
		fmt.Println("consume: inputs ok")
		return flow.WriteOutput("consumed", "ok")
	case "secret":
		v := os.Getenv("E2E_SECRET")
		sum := sha256.Sum256([]byte(v))
		if v == "" || hex.EncodeToString(sum[:]) != os.Getenv("E2E_SECRET_SHA") {
			return errors.New("E2E_SECRET missing or wrong")
		}
		fmt.Printf("secret is %s (len %d)\n", v, len(v))
		return nil
	case "nosecret":
		if _, ok := os.LookupEnv("E2E_SECRET"); ok {
			return errors.New("E2E_SECRET reached a job that does not name it")
		}
		for _, kv := range os.Environ() {
			if strings.HasPrefix(kv, "HOURCH_") && !strings.HasPrefix(kv, "HOURCH_URL=") && !strings.HasPrefix(kv, "HOURCH_JOB_TOKEN=") &&
				!strings.HasPrefix(kv, "HOURCH_OUTPUTS=") && !strings.HasPrefix(kv, "HOURCH_RUN_ID=") &&
				!strings.HasPrefix(kv, "HOURCH_JOB_ID=") && !strings.HasPrefix(kv, "HOURCH_ATTEMPT=") {
				return fmt.Errorf("unexpected %s in the job environment", strings.SplitN(kv, "=", 2)[0])
			}
		}
		fmt.Println("nosecret: clean environment")
		return nil
	case "record":
		// Counters and builds with HOURCH_JOB_TOKEN, as RecordBuild will.
		jc, err := flow.NewJobClientFromEnv()
		if err != nil {
			return err
		}
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		num, _, err := jc.RecordBuild(ctx, "e2e", flow.BuildFlavour("debian", "amd64", ""), func(n int64) any {
			return map[string]any{"build_num": n, "note": "from e2e"}
		})
		if err != nil {
			return err
		}
		if _, err := jc.NextCounter(ctx, "e2e-extra"); err != nil {
			return err
		}
		if err := touch("job.token", os.Getenv(flow.EnvJobToken)); err != nil {
			return err
		}
		fmt.Printf("record: build %d\n", num)
		return flow.WriteOutput("build_num", fmt.Sprint(num))
	case "lock":
		// Fails if another holder of the same concurrency key runs now.
		f, err := os.OpenFile(shared("lock.guard"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o644)
		if err != nil {
			return fmt.Errorf("concurrency key not exclusive: %v", err)
		}
		f.Close()
		fmt.Println("lock: holding", args)
		time.Sleep(400 * time.Millisecond)
		return os.Remove(shared("lock.guard"))
	case "block":
		// Signals it started, then waits for the test to release it.
		name := args[0]
		fmt.Println("block: started", name)
		if err := touch(name+".started", os.Getenv(flow.EnvJobJobID)); err != nil {
			return err
		}
		if err := waitFile(name+".release", 60*time.Second); err != nil {
			return err
		}
		fmt.Println("block: released", name)
		return flow.WriteOutput("survived", "yes")
	case "victim":
		// Attempt 1 hangs until its worker is killed; attempt 2 succeeds.
		attempt := os.Getenv(flow.EnvJobAttempt)
		if attempt == "1" {
			if err := touch("victim.started", os.Getenv(flow.EnvJobJobID)); err != nil {
				return err
			}
			time.Sleep(time.Hour)
			return errors.New("not killed")
		}
		fmt.Println("victim: attempt", attempt)
		return nil
	case "noop":
		fmt.Println("noop")
		return nil
	case "fail":
		fmt.Println("fail: about to exit 3")
		os.Exit(3)
	}
	return fmt.Errorf("unknown mode %q", mode)
}
