// Package secrets seals run secrets for the object store under a volatile
// key, and redacts known values from log text.
//
// The key is 32 bytes from crypto/rand, generated when hourchd starts and
// held only in memory: it is never logged, written or read from anywhere. A
// restart makes every sealed object unreadable. Each hourchd process also has an instance id
// (a ULID); a run's secrets live in one object, secrets/<instance>/<run>,
// so objects written by an earlier process are recognisable by key alone.
//
// An object is 1 version byte, a fresh 12-byte nonce and the AES-256-GCM
// sealing of the run's name → value map (JSON). The additional data binds
// the object to its instance id and run id, so an object copied to another
// run's key, or presented to another instance, does not open.
package secrets

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"strings"
)

// KeySize is the AES-256 key length.
const KeySize = 32

// Prefix is the object-store prefix of every sealed run-secrets object.
const Prefix = "secrets/"

const version byte = 1

// ErrUnreadable: the object was written by another instance (an earlier
// hourchd process), or it does not decrypt under this instance's key.
var ErrUnreadable = errors.New("secrets: object not readable by this hourchd instance")

// Box seals and opens run-secrets objects for one hourchd instance.
type Box struct {
	aead     cipher.AEAD
	instance string
}

// NewVolatile returns a Box with a fresh random key for instance. The key
// bytes are not kept outside the cipher.
func NewVolatile(instance string) (*Box, error) {
	key := make([]byte, KeySize)
	if _, err := rand.Read(key); err != nil {
		return nil, fmt.Errorf("secrets: generating the key: %w", err)
	}
	defer clear(key)
	return newBox(key, instance)
}

// NewWithKey returns a Box for a given key. Tests use it to know the key
// (to prove it never reaches a log); hourchd itself uses NewVolatile.
func NewWithKey(key []byte, instance string) (*Box, error) { return newBox(key, instance) }

func newBox(key []byte, instance string) (*Box, error) {
	if len(key) != KeySize {
		return nil, fmt.Errorf("secrets: key must be %d bytes, got %d", KeySize, len(key))
	}
	if instance == "" || strings.Contains(instance, "/") {
		return nil, fmt.Errorf("secrets: bad instance id %q", instance)
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	return &Box{aead: aead, instance: instance}, nil
}

// Instance is the instance id this Box seals for.
func (b *Box) Instance() string { return b.instance }

// String never shows key material.
func (b *Box) String() string { return "secrets.Box{instance=" + b.instance + "}" }

// GoString never shows key material (%#v).
func (b *Box) GoString() string { return b.String() }

// LogValue never shows key material (slog).
func (b *Box) LogValue() slog.Value { return slog.StringValue(b.String()) }

// ObjectKey is the object key of a run's secrets written by instance.
func ObjectKey(instance, runID string) string { return Prefix + instance + "/" + runID }

// ParseObjectKey splits an object key under Prefix into instance and run id.
func ParseObjectKey(key string) (instance, runID string, ok bool) {
	rest, found := strings.CutPrefix(key, Prefix)
	if !found {
		return "", "", false
	}
	instance, runID, found = strings.Cut(rest, "/")
	if !found || instance == "" || runID == "" || strings.Contains(runID, "/") {
		return "", "", false
	}
	return instance, runID, true
}

// Key is the object key of runID's secrets for this instance.
func (b *Box) Key(runID string) string { return ObjectKey(b.instance, runID) }

func aad(instance, runID string) []byte {
	return []byte("hourchd-run-secrets-v1\x00" + instance + "\x00" + runID)
}

// Seal encrypts a run's secrets for this instance.
func (b *Box) Seal(runID string, values map[string]string) ([]byte, error) {
	pt, err := json.Marshal(values)
	if err != nil {
		return nil, err
	}
	defer clear(pt)
	ns := b.aead.NonceSize()
	out := make([]byte, 1+ns, 1+ns+len(pt)+b.aead.Overhead())
	out[0] = version
	if _, err := rand.Read(out[1:]); err != nil {
		return nil, err
	}
	return b.aead.Seal(out, out[1:], pt, aad(b.instance, runID)), nil
}

// Open decrypts the object at ObjectKey(instance, runID). An object of
// another instance, or one that does not decrypt, is ErrUnreadable.
func (b *Box) Open(instance, runID string, obj []byte) (map[string]string, error) {
	if instance != b.instance {
		return nil, fmt.Errorf("%w: written by instance %s, this is %s", ErrUnreadable, instance, b.instance)
	}
	ns := b.aead.NonceSize()
	if len(obj) < 1+ns || obj[0] != version {
		return nil, fmt.Errorf("%w: bad header", ErrUnreadable)
	}
	pt, err := b.aead.Open(nil, obj[1:1+ns], obj[1+ns:], aad(instance, runID))
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrUnreadable, err)
	}
	defer clear(pt)
	var m map[string]string
	if err := json.Unmarshal(pt, &m); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrUnreadable, err)
	}
	if m == nil {
		m = map[string]string{}
	}
	return m, nil
}

// Mask replaces a redacted value.
const Mask = "***"

// Redactor replaces exact secret values with Mask.
type Redactor struct {
	r *strings.Replacer
}

// NewRedactor returns a Redactor for the given values. Empty values are
// ignored; longer values are replaced first so a value containing another is
// masked whole.
func NewRedactor(values []string) *Redactor {
	vals := make([]string, 0, len(values))
	for _, v := range values {
		if v != "" {
			vals = append(vals, v)
		}
	}
	if len(vals) == 0 {
		return &Redactor{}
	}
	sort.Slice(vals, func(i, j int) bool { return len(vals[i]) > len(vals[j]) })
	pairs := make([]string, 0, 2*len(vals))
	for _, v := range vals {
		pairs = append(pairs, v, Mask)
	}
	return &Redactor{r: strings.NewReplacer(pairs...)}
}

// Redact masks every known value in s.
func (r *Redactor) Redact(s string) string {
	if r == nil || r.r == nil {
		return s
	}
	return r.r.Replace(s)
}
