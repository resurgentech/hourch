package httpapi_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/resurgentech/hourch/daemon/internal/app"
	"github.com/resurgentech/hourch/daemon/internal/config"
	"github.com/resurgentech/hourch/daemon/internal/httpapi"
)

// fakeToken is recognisable in any error: no fragment of it may appear.
const fakeToken = "FAKE-TOKEN-7f3a9c1e5b2d"

// tokenFragments are pieces of fakeToken long enough to identify it.
func tokenFragments() []string {
	return []string{fakeToken, "FAKE-TO", "7f3a9c", "c1e5b2d"}
}

// TestLoadTokensNeverEchoesAToken: a malformed tokens file fails to load,
// keeps the "tokens file <path>:" context, and never quotes a token value.
func TestLoadTokensNeverEchoesAToken(t *testing.T) {
	cases := map[string]string{
		"bare token":         fakeToken + "\n",
		"value tagged !!int": "submitters:\n  alice: !!int " + fakeToken + "\n",
		"list not map":       "submitters:\n  - " + fakeToken + "\n",
		"syntax error":       "submitters:\n  alice: \"" + fakeToken + "\n  bob: [\n",
	}
	for name, body := range cases {
		t.Run(name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "tokens.yaml")
			if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
				t.Fatal(err)
			}
			_, err := httpapi.LoadTokens(path)
			if err == nil {
				t.Fatal("malformed file loaded")
			}
			msg := err.Error()
			if !strings.HasPrefix(msg, "tokens file "+path+": ") {
				t.Errorf("error lost its context: %s", msg)
			}
			for _, f := range tokenFragments() {
				if strings.Contains(msg, f) {
					t.Errorf("error echoes the token (%q): %s", f, msg)
				}
			}
			// check-config (app.Check) and serve (app.Build) load the file
			// through the same loader; check-config's path is cheap to run.
			cfg := config.Default()
			cfg.TokensFile = path
			cerr := app.Check(cfg)
			if cerr == nil {
				t.Fatal("check-config accepted a malformed tokens file")
			}
			for _, f := range tokenFragments() {
				if strings.Contains(cerr.Error(), f) {
					t.Errorf("check-config echoes the token (%q): %s", f, cerr)
				}
			}
		})
	}
}
