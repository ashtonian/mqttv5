package tlsrevocation

import (
	"bytes"
	"context"
	"crypto/x509"
	"encoding/asn1"
	"encoding/pem"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"slices"
	"time"
)

// crlEntry is a CRL that passed validation, indexed for lookup.
type crlEntry struct {
	scope   crlScope
	revoked map[string]x509.RevocationListEntry // by serial, base 16
	// expires is when the CRL goes stale.
	expires time.Time
}

// crlScope is what an Issuing Distribution Point extension restricts a
// CRL to (RFC 5280 §5.2.5). The zero value is an unrestricted CRL.
type crlScope struct {
	onlyUserCerts bool
	onlyCACerts   bool
	// points are the distribution point URIs the CRL names for itself;
	// empty when it names none.
	points []string
}

var (
	oidAuthorityKeyID     = asn1.ObjectIdentifier{2, 5, 29, 35}
	oidCRLNumber          = asn1.ObjectIdentifier{2, 5, 29, 20}
	oidDeltaCRLIndicator  = asn1.ObjectIdentifier{2, 5, 29, 27}
	oidIssuingDistPoint   = asn1.ObjectIdentifier{2, 5, 29, 28}
	errDeltaCRL           = errors.New("a delta CRL is not a complete list")
	errUnsupportedCritExt = errors.New("unsupported critical extension")
)

// validateCRL checks that list is a complete, current CRL issued by
// issuer whose extensions this package understands, and indexes it.
// crypto/x509 parses CRL extensions without rejecting unknown critical
// ones, so that check is made here.
func (c *Checker) validateCRL(list *x509.RevocationList, issuer *x509.Certificate) (*crlEntry, error) {
	if !bytes.Equal(list.RawIssuer, issuer.RawSubject) {
		return nil, errors.New("issued by a different CA")
	}
	if err := list.CheckSignatureFrom(issuer); err != nil {
		return nil, fmt.Errorf("signature: %w", err)
	}
	expires, err := c.fresh(list.ThisUpdate, list.NextUpdate)
	if err != nil {
		return nil, fmt.Errorf("CRL %w", err)
	}
	entry := &crlEntry{
		revoked: make(map[string]x509.RevocationListEntry, len(list.RevokedCertificateEntries)),
		expires: expires,
	}
	for _, ext := range list.Extensions {
		switch {
		case ext.Id.Equal(oidIssuingDistPoint):
			if entry.scope, err = parseIssuingDistributionPoint(ext.Value); err != nil {
				return nil, err
			}
		case ext.Id.Equal(oidDeltaCRLIndicator):
			return nil, errDeltaCRL
		case ext.Critical && !ext.Id.Equal(oidAuthorityKeyID) && !ext.Id.Equal(oidCRLNumber):
			return nil, fmt.Errorf("%w %v", errUnsupportedCritExt, ext.Id)
		}
	}
	for _, revoked := range list.RevokedCertificateEntries {
		// The critical entry extension in use, Certificate Issuer,
		// belongs to indirect CRLs, which are not supported.
		for _, ext := range revoked.Extensions {
			if ext.Critical {
				return nil, fmt.Errorf("entry for serial %s: %w %v", serialText(revoked.SerialNumber), errUnsupportedCritExt, ext.Id)
			}
		}
		entry.revoked[serialText(revoked.SerialNumber)] = revoked
	}
	return entry, nil
}

// lookup answers cert's status from a validated CRL. fetchedFrom is the
// distribution point the CRL came from, or empty for one supplied by
// the CRL provider.
func (e *crlEntry) lookup(cert *x509.Certificate, fetchedFrom string, source Source) (answer, error) {
	if e.scope.onlyCACerts && !cert.IsCA {
		return answer{}, errors.New("CRL covers CA certificates only")
	}
	if e.scope.onlyUserCerts && cert.IsCA {
		return answer{}, errors.New("CRL covers end-entity certificates only")
	}
	if fetchedFrom != "" && len(e.scope.points) > 0 && !slices.Contains(e.scope.points, fetchedFrom) {
		return answer{}, fmt.Errorf("CRL names distribution points %v, not %s", e.scope.points, fetchedFrom)
	}
	a := answer{status: statusGood, source: source, expires: e.expires}
	if revoked, ok := e.revoked[serialText(cert.SerialNumber)]; ok {
		a.status = statusRevoked
		a.revokedAt = revoked.RevocationTime
		a.reason = revoked.ReasonCode
	}
	return a, nil
}

// issuingDistributionPoint is the IssuingDistributionPoint extension,
// RFC 5280 §5.2.5. distributionPoint is a tagged CHOICE, so its tag is
// explicit.
type issuingDistributionPoint struct {
	DistributionPoint          asn1.RawValue  `asn1:"optional,explicit,tag:0"`
	OnlyContainsUserCerts      bool           `asn1:"optional,tag:1"`
	OnlyContainsCACerts        bool           `asn1:"optional,tag:2"`
	OnlySomeReasons            asn1.BitString `asn1:"optional,tag:3"`
	IndirectCRL                bool           `asn1:"optional,tag:4"`
	OnlyContainsAttributeCerts bool           `asn1:"optional,tag:5"`
}

// Context-specific tags within the extension.
const (
	tagFullName = 0 // DistributionPointName.fullName
	tagURI      = 6 // GeneralName.uniformResourceIdentifier
)

// parseIssuingDistributionPoint reads the scope of a CRL. CRLs that
// partition revocations by reason, are indirect, or cover attribute
// certificates are rejected: a lookup in one could miss a revocation
// listed in another.
func parseIssuingDistributionPoint(der []byte) (crlScope, error) {
	var idp issuingDistributionPoint
	if rest, err := asn1.Unmarshal(der, &idp); err != nil || len(rest) > 0 {
		return crlScope{}, errors.New("malformed issuing distribution point")
	}
	switch {
	case idp.IndirectCRL:
		return crlScope{}, errors.New("indirect CRLs are not supported")
	case idp.OnlySomeReasons.BitLength > 0:
		return crlScope{}, errors.New("CRLs partitioned by reason are not supported")
	case idp.OnlyContainsAttributeCerts:
		return crlScope{}, errors.New("CRL covers attribute certificates only")
	}
	scope := crlScope{onlyUserCerts: idp.OnlyContainsUserCerts, onlyCACerts: idp.OnlyContainsCACerts}
	name := idp.DistributionPoint
	if name.Class == asn1.ClassContextSpecific && name.Tag == tagFullName && len(name.Bytes) > 0 {
		rest := name.Bytes
		for len(rest) > 0 {
			var general asn1.RawValue
			var err error
			if rest, err = asn1.Unmarshal(rest, &general); err != nil {
				return crlScope{}, errors.New("malformed issuing distribution point name")
			}
			if general.Class == asn1.ClassContextSpecific && general.Tag == tagURI {
				scope.points = append(scope.points, string(general.Bytes))
			}
		}
	}
	return scope, nil
}

// providerCRL answers cert's status from the CRL provider.
func (c *Checker) providerCRL(ctx context.Context, cert, issuer *x509.Certificate) (answer, error) {
	list, err := c.cfg.provider(ctx, issuer)
	if err != nil {
		return answer{}, err
	}
	if list == nil {
		return answer{}, nil
	}
	entry, err := c.validateCRL(list, issuer)
	if err != nil {
		return answer{}, err
	}
	return entry.lookup(cert, "", SourceCRLProvider)
}

// fetchedCRL answers cert's status from the CRL at point, through the
// cache.
func (c *Checker) fetchedCRL(ctx context.Context, log *slog.Logger, point string, cert, issuer *x509.Certificate) (answer, error) {
	key := issuerKey(issuer) + " " + point
	entry, ok := c.crls.get(key, c.cfg.now())
	if ok {
		c.stats.cacheHits.Add(1)
	} else {
		c.stats.cacheMisses.Add(1)
		var err error
		if entry, err = c.fetchCRL(ctx, log, key, point, issuer); err != nil {
			return answer{}, err
		}
	}
	return entry.lookup(cert, point, SourceCRL)
}

// fetchCRL downloads and validates the CRL at point, sharing the fetch
// with concurrent callers, and caches it.
func (c *Checker) fetchCRL(ctx context.Context, log *slog.Logger, key, point string, issuer *x509.Certificate) (*crlEntry, error) {
	if err := checkURL(point); err != nil {
		return nil, err
	}
	if err := c.backingOff(point); err != nil {
		return nil, err
	}
	return c.crlFetches.do(ctx, key, c.cfg.fetchTimeout, func(ctx context.Context) (*crlEntry, error) {
		entry, err := c.downloadCRL(ctx, point, issuer)
		if err != nil {
			c.failed(log, point, err)
			return nil, err
		}
		c.crls.put(key, entry, entry.expires)
		return entry, nil
	})
}

func (c *Checker) downloadCRL(ctx context.Context, point string, issuer *x509.Certificate) (*crlEntry, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, point, nil)
	if err != nil {
		return nil, fmt.Errorf("build request: %w", err)
	}
	body, err := c.fetch(req, c.cfg.maxCRLBytes)
	if err != nil {
		return nil, err
	}
	// Distribution points serve DER (RFC 5280 §4.2.1.13); some serve
	// PEM.
	if block, _ := pem.Decode(body); block != nil && block.Type == "X509 CRL" {
		body = block.Bytes
	}
	list, err := x509.ParseRevocationList(body)
	if err != nil {
		return nil, fmt.Errorf("parse CRL: %w", err)
	}
	return c.validateCRL(list, issuer)
}
