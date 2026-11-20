package httpapi

import (
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"regexp"
	"strings"

	"github.com/resurgentech/keyclient/keylist"
	"gopkg.in/yaml.v3"

	"github.com/resurgentech/hourch/daemon/internal/graph"
)

// quoted matches a backtick-quoted span, which yaml.v3 decode errors use to
// quote the offending value. In the tokens file that value may be a token,
// so it is redacted before the error is returned.
var quoted = regexp.MustCompile("`[^`]*`")

// Token kinds.
const (
	KindSubmitter = "submitter"
	KindWorker    = "worker"
	KindJob       = "job"
)

// TokensFile is tokens_file: submitter/operator tokens and one token per
// worker id, each a name → token map.
//
//	submitters:
//	  alice: <token>
//	workers:
//	  node01: <token>
type TokensFile struct {
	Submitters map[string]string `yaml:"submitters"`
	Workers    map[string]string `yaml:"workers"`
}

// Tokens resolves a presented bearer token to its name and kind. Every
// lookup compares against every token in constant time.
type Tokens struct {
	names  []string
	kinds  []string
	hashes [][32]byte
}

// NewTokens validates the file: at least one token, no empty values, no
// token shared by two names, worker names valid worker ids.
func NewTokens(f TokensFile) (*Tokens, error) {
	if len(f.Submitters)+len(f.Workers) == 0 {
		return nil, errors.New("tokens: no tokens configured (fail closed)")
	}
	t := &Tokens{}
	seen := map[[32]byte]string{}
	add := func(kind string, m map[string]string) error {
		for name, tok := range m {
			if strings.TrimSpace(name) == "" || tok == "" {
				return fmt.Errorf("tokens: empty name or token for %s %q", kind, name)
			}
			if kind == KindWorker && !graph.WorkerIDRe.MatchString(name) {
				return fmt.Errorf("tokens: worker id %q does not match %s", name, graph.WorkerIDRe)
			}
			if strings.HasPrefix(tok, "hjt_") {
				return fmt.Errorf("tokens: %s %q: the hjt_ prefix is reserved for job tokens", kind, name)
			}
			h := sha256.Sum256([]byte(tok))
			if prev, ok := seen[h]; ok {
				return fmt.Errorf("tokens: %s and %s share a token", prev, name)
			}
			seen[h] = name
			t.names, t.kinds, t.hashes = append(t.names, name), append(t.kinds, kind), append(t.hashes, h)
		}
		return nil
	}
	if err := add(KindSubmitter, f.Submitters); err != nil {
		return nil, err
	}
	if err := add(KindWorker, f.Workers); err != nil {
		return nil, err
	}
	return t, nil
}

// LoadTokens reads tokens_file. It must not be readable by group or other.
func LoadTokens(path string) (*Tokens, error) {
	fi, err := os.Stat(path)
	if err != nil {
		return nil, err
	}
	if fi.Mode().Perm()&0o077 != 0 {
		return nil, fmt.Errorf("tokens file %s has mode %v; want 0600", path, fi.Mode().Perm())
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var f TokensFile
	dec := yaml.NewDecoder(strings.NewReader(string(data)))
	dec.KnownFields(true)
	if err := dec.Decode(&f); err != nil {
		return nil, fmt.Errorf("tokens file %s: %s", path, quoted.ReplaceAllString(err.Error(), "`<redacted>`"))
	}
	return NewTokens(f)
}

// Lookup returns the name and kind for a presented token.
func (t *Tokens) Lookup(presented string) (name, kind string, ok bool) {
	h := sha256.Sum256([]byte(presented))
	match := -1
	for i := range t.hashes {
		if subtle.ConstantTimeCompare(h[:], t.hashes[i][:]) == 1 {
			match = i
		}
	}
	if match < 0 || presented == "" {
		return "", "", false
	}
	return t.names[match], t.kinds[match], true
}

// TakenLocally reports whether a key-list entry clashes with a token loaded
// from tokens_file: the same name as any submitter or worker, or the same
// SHA-256 as any token. Local tokens win. The tokens are fixed after
// construction, so the answer never changes for a given entry (keylist
// requires that of its Accept function).
func (t *Tokens) TakenLocally(e keylist.Entry) bool {
	want := strings.ToLower(e.SHA256)
	for i := range t.names {
		if e.Name == t.names[i] || want == hex.EncodeToString(t.hashes[i][:]) {
			return true
		}
	}
	return false
}
