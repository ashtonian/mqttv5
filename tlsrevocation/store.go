package tlsrevocation

import (
	"context"
	"crypto/sha256"
	"crypto/x509"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"time"
)

// Store keeps the OCSP responses and CRLs the checker fetches, beneath
// its in-memory cache, so they outlive the process or are shared between
// processes: a client that restarts while the responders are unreachable
// can still check a broker against a response it fetched before. The
// checker consults the Store when its cache misses, before any network
// fetch, and writes every OCSP response and CRL it fetches. Set one with
// [WithStore]; package storetest checks an implementation.
//
// Values are the DER the responder or distribution point served,
// signature included, and the checker verifies every value it reads back
// as it verifies a fetch: issuer, signature, freshness and size. A Store
// is therefore not trusted with the verdict. A value that fails is
// counted in [Stats].StoreRejects and replaced by a fetch; the most a
// Store can do is replay a response that is still current, as anyone on
// the path of a plain-HTTP fetch can.
//
// Keys are "ocsp-" or "crl-" followed by 64 lowercase hexadecimal
// digits, usable as file names. The checker does not modify a value
// after passing it to Put or after Get returns it, and a Store must not
// modify a value it has returned.
//
// A Store must be safe for concurrent use. Each call's context is
// bounded by [WithFetchTimeout]. A Store error never fails a check: it
// is logged, counted in [Stats].StoreErrors, and treated as a miss.
type Store interface {
	// Get returns the value stored under key; ok is false when there is
	// none. It may return a value past its expiry, which the checker
	// rejects.
	Get(ctx context.Context, key string) (value []byte, ok bool, err error)

	// Put stores value under key, replacing any value there. expires is
	// when the value goes stale; the Store may drop it any time after.
	Put(ctx context.Context, key string, value []byte, expires time.Time) error
}

// ocspKey identifies the OCSP answers about cert: its issuer and serial
// number, the CertID of RFC 6960 §4.1.1.
func ocspKey(issuer, cert *x509.Certificate) string {
	return entryKey("ocsp", issuer, serialText(cert.SerialNumber))
}

// crlKey identifies the CRL issuer publishes at point.
func crlKey(issuer *x509.Certificate, point string) string {
	return entryKey("crl", issuer, point)
}

// entryKey hashes the issuer, by name and key as a CertID identifies it,
// and id into a fixed-length key. The issuer's two hashes are fixed
// length, so the concatenation is unambiguous.
func entryKey(kind string, issuer *x509.Certificate, id string) string {
	name := sha256.Sum256(issuer.RawSubject)
	key := sha256.Sum256(issuer.RawSubjectPublicKeyInfo)
	h := sha256.New()
	h.Write(name[:])
	h.Write(key[:])
	_, _ = io.WriteString(h, id)
	return kind + "-" + hex.EncodeToString(h.Sum(nil))
}

// errNotStored reports a Store miss.
var errNotStored = errors.New("not in the store")

// load reads key from the Store. A value over max bytes is rejected
// unread. The caller verifies the value and reports the outcome with
// storeHit or storeRejected.
func (c *Checker) load(ctx context.Context, log *slog.Logger, key string, max int64) ([]byte, error) {
	der, ok, err := c.cfg.store.Get(ctx, key)
	switch {
	case err != nil:
		c.stats.storeErrors.Add(1)
		log.WarnContext(ctx, "tlsrevocation: store get failed", slog.String("key", key), slog.Any("error", err))
		return nil, err
	case !ok:
		c.stats.storeMisses.Add(1)
		return nil, errNotStored
	case int64(len(der)) > max:
		err := fmt.Errorf("%w (%d bytes)", errTooLarge, max)
		c.storeRejected(ctx, log, key, err)
		return nil, err
	}
	return der, nil
}

func (c *Checker) storeHit(ctx context.Context, log *slog.Logger, key string) {
	c.stats.storeHits.Add(1)
	log.DebugContext(ctx, "tlsrevocation: store hit", slog.String("key", key))
}

// storeRejected records a stored value that failed verification. A
// stale value is routine for a Store that keeps entries past their
// expiry; anything else means the Store returned data the checker did
// not write.
func (c *Checker) storeRejected(ctx context.Context, log *slog.Logger, key string, err error) {
	c.stats.storeRejects.Add(1)
	level := slog.LevelWarn
	if errors.Is(err, errStale) {
		level = slog.LevelDebug
	}
	log.Log(ctx, level, "tlsrevocation: stored entry rejected", slog.String("key", key), slog.Any("error", err))
}

// save writes a fetched value to the Store, when there is one, under its
// own deadline: the fetch that produced it may have used most of its
// own.
func (c *Checker) save(ctx context.Context, log *slog.Logger, key string, der []byte, expires time.Time) {
	if c.cfg.store == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), c.cfg.fetchTimeout)
	defer cancel()
	if err := c.cfg.store.Put(ctx, key, der, expires); err != nil {
		c.stats.storeErrors.Add(1)
		log.WarnContext(ctx, "tlsrevocation: store put failed", slog.String("key", key), slog.Any("error", err))
	}
}
