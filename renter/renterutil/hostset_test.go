package renterutil

import (
	"encoding/hex"
	"errors"
	"os"
	"strings"
	"testing"

	"go.sia.tech/siad/crypto"
	"go.sia.tech/siad/types"

	"lukechampine.com/frand"
	"lukechampine.com/us/ghost"
	"lukechampine.com/us/hostdb"
)

func generateHostKey(t *testing.T) hostdb.HostPublicKey {
	t.Helper()
	_, pk := crypto.GenerateKeyPair()
	return hostdb.HostKeyFromSiaPublicKey(types.Ed25519PublicKey(pk))
}

func TestHostError(t *testing.T) {
	hostKey := generateHostKey(t)
	e := &HostError{
		HostKey: hostKey,
		Err:     ErrHostAcquired,
	}
	if !strings.Contains(e.Error(), hostKey.ShortKey()) {
		t.Errorf("expect %q contains %q", e.Error(), hostKey.ShortKey())
	}
	if !errors.Is(e, ErrHostAcquired) {
		t.Errorf("expect %v is a %v", e, ErrHostAcquired)
	}
}

func TestHostErrorSet(t *testing.T) {
	errorSize := 40
	hes := make(HostErrorSet, errorSize)
	for i := range hes {
		hes[i] = &HostError{
			HostKey: generateHostKey(t),
			Err:     ErrHostAcquired,
		}
	}

	msg := hes.Error()
	for _, e := range hes {
		if !strings.Contains(msg, e.Error()) {
			t.Errorf("error text doesn't contain the error for %v", e.HostKey.ShortKey())
		}
		if !errors.Is(hes, e) {
			t.Errorf("expect %v is an %v", hes, e)
		}
	}

	errs := hes.Errors()
	if len(hes) != len(errs) {
		t.Errorf("expect %v, got %v", len(hes), len(errs))
	}
	for i, e := range errs {
		if !errors.Is(e, hes[i]) {
			t.Errorf("expect %v, got %v", hes[i], e)
		}
	}
}

func TestHostSet(t *testing.T) {
	hosts := make([]*ghost.Host, 3)
	hkr := make(testHKR)
	hs := NewHostSet(hkr, 0)
	for i := range hosts {
		h, c := createHostWithContract(t)
		hosts[i] = h
		hkr[h.PublicKey] = h.Settings.NetAddress
		hs.AddHost(c)
		if err := h.Close(); err != nil {
			t.Error(err)
		}
	}

	fs := NewFileSystem(os.TempDir(), hs)
	defer func() {
		_ = fs.Close()
		for _, h := range hosts {
			if err := h.Close(); err != nil {
				t.Error(err)
			}
		}
	}()

	metaName := t.Name() + "-" + hex.EncodeToString(frand.Bytes(6))
	pf, err := fs.Create(metaName, 2)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pf.Write([]byte("foo")); err != nil {
		t.Fatal(err)
	}
	// should get a HostErrorSet when we sync
	err = pf.Sync()

	var hes HostErrorSet

	ok := errors.As(err, &hes)
	if !ok || hes == nil {
		t.Fatal("expected HostSetError")
	} else if len(hes) != 3 {
		t.Fatal("expected HostSetError to have three hosts, got", len(hes))
	}
}
