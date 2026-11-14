package tlsrevocation

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/x509"
	"encoding/asn1"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"slices"
	"strings"
	"time"

	"golang.org/x/crypto/ocsp"
)

// parseOCSP verifies an OCSP response for cert and turns it into an
// answer. ocsp.ParseResponseForCert checks the signatures, by the issuer
// or by a responder certificate the issuer signed; it does not check
// that a delegated responder is authorised to sign OCSP responses or is
// within its validity window (RFC 6960 §4.2.2.2), nor that the response
// is fresh. Those checks are made here.
func (c *Checker) parseOCSP(der []byte, cert, issuer *x509.Certificate) (answer, error) {
	resp, err := ocsp.ParseResponseForCert(der, cert, issuer)
	if err != nil {
		return answer{}, err
	}
	if responder := resp.Certificate; responder != nil && !responder.Equal(issuer) {
		if !slices.Contains(responder.ExtKeyUsage, x509.ExtKeyUsageOCSPSigning) {
			return answer{}, errors.New("delegated responder certificate is not authorised for OCSP signing")
		}
		now := c.cfg.now()
		if now.Add(c.cfg.skew).Before(responder.NotBefore) || now.Add(-c.cfg.skew).After(responder.NotAfter) {
			return answer{}, errors.New("delegated responder certificate is outside its validity window")
		}
	}
	expires, err := c.fresh(resp.ThisUpdate, resp.NextUpdate)
	if err != nil {
		return answer{}, fmt.Errorf("response %w", err)
	}
	a := answer{expires: expires}
	switch resp.Status {
	case ocsp.Good:
		a.status = statusGood
	case ocsp.Revoked:
		a.status = statusRevoked
		a.revokedAt = resp.RevokedAt
		a.reason = resp.RevocationReason
	default:
		a.status = statusUnknown
	}
	return a, nil
}

// fetchedOCSP asks the responder at server for cert's status, through
// the cache.
func (c *Checker) fetchedOCSP(ctx context.Context, log *slog.Logger, server string, cert, issuer *x509.Certificate) (answer, error) {
	key := certKey(issuer, cert)
	if a, ok := c.ocspAnswers.get(key, c.cfg.now()); ok {
		c.stats.cacheHits.Add(1)
		return a, nil
	}
	c.stats.cacheMisses.Add(1)
	if err := checkURL(server); err != nil {
		return answer{}, err
	}
	if err := c.backingOff(server); err != nil {
		return answer{}, err
	}
	return c.ocspFetches.do(ctx, key+" "+server, c.cfg.fetchTimeout, func(ctx context.Context) (answer, error) {
		der, err := c.requestOCSP(ctx, server, cert, issuer)
		var a answer
		if err == nil {
			a, err = c.parseOCSP(der, cert, issuer)
		}
		if err != nil {
			c.failed(log, server, err)
			return answer{}, err
		}
		a.source = SourceOCSP
		if a.status != statusUnknown {
			c.ocspAnswers.put(key, a, a.expires)
		}
		return a, nil
	})
}

// maxGETRequest is the longest encoded request sent by GET (RFC 6960
// §A.1); longer ones are POSTed.
const maxGETRequest = 255

var base64URLEscaper = strings.NewReplacer("+", "%2B", "/", "%2F", "=", "%3D")

func (c *Checker) requestOCSP(ctx context.Context, server string, cert, issuer *x509.Certificate) ([]byte, error) {
	reqDER, err := ocsp.CreateRequest(cert, issuer, nil)
	if err != nil {
		return nil, fmt.Errorf("build request: %w", err)
	}
	var req *http.Request
	if encoded := base64URLEscaper.Replace(base64.StdEncoding.EncodeToString(reqDER)); len(encoded) <= maxGETRequest {
		req, err = http.NewRequestWithContext(ctx, http.MethodGet, strings.TrimSuffix(server, "/")+"/"+encoded, nil)
	} else {
		req, err = http.NewRequestWithContext(ctx, http.MethodPost, server, bytes.NewReader(reqDER))
		if req != nil {
			req.Header.Set("Content-Type", "application/ocsp-request")
		}
	}
	if err != nil {
		return nil, fmt.Errorf("build request: %w", err)
	}
	req.Header.Set("Accept", "application/ocsp-response")
	return c.fetch(req, c.cfg.maxOCSPBytes)
}

// errTooLarge reports a response over its size cap.
var errTooLarge = errors.New("response exceeds the size limit")

// fetch performs req and returns the body of a 200 response, at most
// max bytes.
func (c *Checker) fetch(req *http.Request, max int64) ([]byte, error) {
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("HTTP %s", resp.Status)
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, max+1))
	if err != nil {
		return nil, fmt.Errorf("read response: %w", err)
	}
	if int64(len(body)) > max {
		return nil, fmt.Errorf("%w (%d bytes)", errTooLarge, max)
	}
	return body, nil
}

// checkURL admits the http and https URLs OCSP responders and CRL
// distribution points are fetched from.
func checkURL(raw string) error {
	u, err := url.Parse(raw)
	if err != nil {
		return fmt.Errorf("invalid URL: %w", err)
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return fmt.Errorf("unsupported URL scheme %q", u.Scheme)
	}
	return nil
}

// maxRedirects bounds the redirects a fetch follows.
const maxRedirects = 3

func defaultHTTPClient(timeout time.Duration) *http.Client {
	return &http.Client{
		Timeout: timeout,
		Transport: &http.Transport{
			Proxy:                 http.ProxyFromEnvironment,
			DialContext:           (&net.Dialer{Timeout: timeout, KeepAlive: 30 * time.Second}).DialContext,
			TLSHandshakeTimeout:   timeout,
			ResponseHeaderTimeout: timeout,
			MaxIdleConns:          16,
			MaxIdleConnsPerHost:   2,
			IdleConnTimeout:       90 * time.Second,
		},
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			if len(via) >= maxRedirects {
				return fmt.Errorf("stopped after %d redirects", maxRedirects)
			}
			return checkURL(req.URL.String())
		},
	}
}

// certKey identifies cert by its issuer's key and its serial number,
// the pair OCSP answers are about.
func certKey(issuer, cert *x509.Certificate) string {
	return issuerKey(issuer) + ":" + serialText(cert.SerialNumber)
}

func issuerKey(issuer *x509.Certificate) string {
	sum := sha256.Sum256(issuer.RawSubjectPublicKeyInfo)
	return hex.EncodeToString(sum[:])
}

// oidTLSFeature is the TLS Feature extension (RFC 7633).
var oidTLSFeature = asn1.ObjectIdentifier{1, 3, 6, 1, 5, 5, 7, 1, 24}

// statusRequest is the status_request TLS extension number, which a TLS
// Feature extension lists to require a stapled OCSP response.
const statusRequest = 5

// mustStaple reports whether cert requires a stapled OCSP response. A
// malformed TLS Feature extension is read as requiring one.
func mustStaple(cert *x509.Certificate) bool {
	for _, ext := range cert.Extensions {
		if !ext.Id.Equal(oidTLSFeature) {
			continue
		}
		var features []int
		if rest, err := asn1.Unmarshal(ext.Value, &features); err != nil || len(rest) > 0 {
			return true
		}
		return slices.Contains(features, statusRequest)
	}
	return false
}
