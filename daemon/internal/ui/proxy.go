package ui

import (
	"bufio"
	"bytes"
	"crypto/sha256"
	"crypto/subtle"
	"errors"
	"fmt"
	"net/http"
	"os"
	"regexp"
	"strings"
)

// ProxyUserRe is the rule for a proxied username (docs/DESIGN-hourchd.md
// "Web UI"): 1 to 64 of letters, digits, and _ . @ -.
var ProxyUserRe = regexp.MustCompile(`^[A-Za-z0-9_.@-]{1,64}$`)

// ProxyAuth authenticates UI requests that a trusted reverse proxy has
// already authenticated: the proxy sets the user header and a shared-secret
// header, and the username is trusted only when the secret matches a line of
// the secret file.
type ProxyAuth struct {
	secretHeader string // canonical form
	userHeader   string // canonical form
	secrets      [][sha256.Size]byte
}

// MinProxySecretLen is the shortest secret the secret file may hold.
const MinProxySecretLen = 16

// NewProxyAuth reads the secret file (one secret per line; surrounding
// whitespace trimmed; blank lines and lines starting with # skipped; each
// secret at least MinProxySecretLen characters). The file must have no group
// or other permission bits (mode & 0077 == 0), and must hold at least one
// secret. Secret values never appear in errors.
func NewProxyAuth(secretFile, secretHeader, userHeader string) (*ProxyAuth, error) {
	if secretFile == "" || secretHeader == "" || userHeader == "" {
		return nil, errors.New("ui.proxy: secret_file, secret_header and user_header are required")
	}
	fi, err := os.Stat(secretFile)
	if err != nil {
		return nil, fmt.Errorf("ui.proxy.secret_file: %w", err)
	}
	if fi.Mode().Perm()&0o077 != 0 {
		return nil, fmt.Errorf("ui.proxy.secret_file %s has mode %04o; it must have no group or other permissions (want 0600)", secretFile, fi.Mode().Perm())
	}
	data, err := os.ReadFile(secretFile)
	if err != nil {
		return nil, fmt.Errorf("ui.proxy.secret_file: %w", err)
	}
	p := &ProxyAuth{
		secretHeader: http.CanonicalHeaderKey(secretHeader),
		userHeader:   http.CanonicalHeaderKey(userHeader),
	}
	sc := bufio.NewScanner(bytes.NewReader(data))
	sc.Buffer(nil, 1<<20)
	for n := 1; sc.Scan(); n++ {
		line := strings.TrimSpace(sc.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		if len(line) < MinProxySecretLen {
			return nil, fmt.Errorf("ui.proxy.secret_file %s: line %d: a secret must be at least %d characters", secretFile, n, MinProxySecretLen)
		}
		p.secrets = append(p.secrets, sha256.Sum256([]byte(line)))
	}
	if err := sc.Err(); err != nil {
		return nil, fmt.Errorf("ui.proxy.secret_file %s: %w", secretFile, err)
	}
	if len(p.secrets) == 0 {
		return nil, fmt.Errorf("ui.proxy.secret_file %s holds no secret", secretFile)
	}
	return p, nil
}

// secretOK reports whether v equals a line of the secret file. It hashes v
// and compares against every stored hash, with no early exit.
func (p *ProxyAuth) secretOK(v string) bool {
	h := sha256.Sum256([]byte(v))
	ok := 0
	for i := range p.secrets {
		ok |= subtle.ConstantTimeCompare(h[:], p.secrets[i][:])
	}
	return ok == 1
}

// User returns the proxied username of r, or false when r does not carry the
// secret header and the user header exactly once each, with a matching secret
// and a valid name.
func (p *ProxyAuth) User(r *http.Request) (string, bool) {
	s, u := r.Header.Values(p.secretHeader), r.Header.Values(p.userHeader)
	if len(s) != 1 || len(u) != 1 {
		return "", false
	}
	// Check the secret first and always, so timing does not depend on the name.
	if !p.secretOK(s[0]) || !ProxyUserRe.MatchString(u[0]) {
		return "", false
	}
	return u[0], true
}

// strip removes the proxy headers from h.
func (p *ProxyAuth) strip(h http.Header) {
	h.Del(p.secretHeader)
	h.Del(p.userHeader)
}
