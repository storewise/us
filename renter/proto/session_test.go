package proto

import (
	"bytes"
	"crypto/ed25519"
	"io/ioutil"
	"testing"

	"gitlab.com/NebulousLabs/Sia/crypto"
	"gitlab.com/NebulousLabs/Sia/encoding"
	"gitlab.com/NebulousLabs/Sia/modules"
	"gitlab.com/NebulousLabs/Sia/types"
	"lukechampine.com/us/internal/ghost"
	"lukechampine.com/us/renterhost"
)

func deepEqual(a, b interface{}) bool {
	return bytes.Equal(encoding.Marshal(a), encoding.Marshal(b))
}

type stubWallet struct{}

func (stubWallet) NewWalletAddress() (uh types.UnlockHash, err error)                       { return }
func (stubWallet) SignTransaction(*types.Transaction, []crypto.Hash) (err error)            { return }
func (stubWallet) UnspentOutputs(bool) (us []modules.UnspentOutput, err error)              { return }
func (stubWallet) UnconfirmedParents(types.Transaction) (ps []types.Transaction, err error) { return }
func (stubWallet) UnlockConditions(types.UnlockHash) (uc types.UnlockConditions, err error) { return }

type stubTpool struct{}

func (stubTpool) AcceptTransactionSet([]types.Transaction) (err error) { return }
func (stubTpool) FeeEstimate() (min, max types.Currency, err error)    { return }

// createTestingPair creates a renter and host, initiates a Session between
// them, and forms and locks a contract.
func createTestingPair(tb testing.TB) (*Session, *ghost.Host) {
	tb.Helper()

	host, err := ghost.New(":0")
	if err != nil {
		tb.Fatal(err)
	}

	s, err := NewUnlockedSession(host.Settings().NetAddress, host.PublicKey(), 0)
	if err != nil {
		tb.Fatal(err)
	}

	settings, err := s.Settings()
	if err != nil {
		tb.Fatal(err)
	}
	if !deepEqual(settings, host.Settings()) {
		tb.Fatal("received settings do not match host's actual settings")
	}

	key := ed25519.NewKeyFromSeed(make([]byte, ed25519.SeedSize))
	rev, _, err := s.FormContract(stubWallet{}, stubTpool{}, key, types.ZeroCurrency, 0, 0)
	if err != nil {
		tb.Fatal(err)
	}
	err = s.Lock(rev.ID(), key, 0)
	if err != nil {
		tb.Fatal(err)
	}
	return s, host
}

type testStatsRecorder struct {
	stats []RPCStats
}

func (tsr *testStatsRecorder) RecordRPCStats(stats RPCStats) { tsr.stats = append(tsr.stats, stats) }

func TestSession(t *testing.T) {
	renter, host := createTestingPair(t)
	defer renter.Close()
	defer host.Close()

	var tsr testStatsRecorder
	renter.SetRPCStatsRecorder(&tsr)

	sector := [renterhost.SectorSize]byte{0: 1}
	sectorRoot, err := renter.Append(&sector)
	if err != nil {
		t.Fatal(err)
	}
	if len(tsr.stats) != 1 {
		t.Fatal("no stats collected")
	} else if stats := tsr.stats[0]; stats.Host != host.PublicKey() ||
		stats.RPC != renterhost.RPCWriteID ||
		stats.Uploaded == 0 || stats.Downloaded == 0 {
		t.Fatal("bad stats:", stats)
	}

	roots, err := renter.SectorRoots(0, 1)
	if err != nil {
		t.Fatal(err)
	} else if roots[0] != sectorRoot {
		t.Fatal("reported sector root does not match actual sector root")
	}
	if len(tsr.stats) != 2 {
		t.Fatal("no stats collected")
	} else if stats := tsr.stats[1]; stats.Host != host.PublicKey() ||
		stats.RPC != renterhost.RPCSectorRootsID ||
		stats.Uploaded == 0 || stats.Downloaded == 0 {
		t.Fatal("bad stats:", stats)
	}

	var sectorBuf bytes.Buffer
	err = renter.Read(&sectorBuf, []renterhost.RPCReadRequestSection{{
		MerkleRoot: sectorRoot,
		Offset:     0,
		Length:     renterhost.SectorSize,
	}})
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(sectorBuf.Bytes(), sector[:]) {
		t.Fatal("downloaded sector does not match uploaded sector")
	}
	if len(tsr.stats) != 3 {
		t.Fatal("no stats collected")
	} else if stats := tsr.stats[2]; stats.Host != host.PublicKey() ||
		stats.RPC != renterhost.RPCReadID ||
		stats.Uploaded == 0 || stats.Downloaded == 0 {
		t.Fatal("bad stats:", stats)
	}

	err = renter.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	if len(tsr.stats) != 4 {
		t.Fatal("no stats collected")
	} else if stats := tsr.stats[3]; stats.Host != host.PublicKey() ||
		stats.RPC != renterhost.RPCUnlockID ||
		stats.Uploaded == 0 || stats.Downloaded != 0 {
		t.Fatal("bad stats:", stats)
	}
}

func TestRenewAndClear(t *testing.T) {
	renter, host := createTestingPair(t)
	defer renter.Close()
	defer host.Close()

	sector := [renterhost.SectorSize]byte{0: 1}
	sectorRoot, err := renter.Append(&sector)
	if err != nil {
		t.Fatal(err)
	}

	newContract, _, err := renter.RenewAndClearContract(stubWallet{}, stubTpool{}, types.ZeroCurrency, 0, 0)
	if err != nil {
		t.Fatal(err)
	}

	// attempting to revise the old contract should cause an error
	err = renter.Read(ioutil.Discard, []renterhost.RPCReadRequestSection{{
		MerkleRoot: sectorRoot,
		Offset:     0,
		Length:     renterhost.SectorSize,
	}})
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	oldID, oldKey := renter.Revision().ID(), renter.key
	renter, err = NewUnlockedSession(host.Settings().NetAddress, host.PublicKey(), 0)
	if err != nil {
		t.Fatal(err)
	}

	// attempting to lock the old contract should cause an error
	err = renter.Lock(oldID, oldKey, 0)
	if err == ErrContractFinalized {
		t.Fatal("expected ErrContractFinalized, got", err)
	}
	renter, err = NewUnlockedSession(host.Settings().NetAddress, host.PublicKey(), 0)
	if err != nil {
		t.Fatal(err)
	}

	// we should be able to lock and revise the new contract, and its roots
	// should be the same as the old contract
	if err := renter.Lock(newContract.ID(), oldKey, 0); err != nil {
		t.Fatal(err)
	}

	roots, err := renter.SectorRoots(0, 1)
	if err != nil {
		t.Fatal(err)
	} else if roots[0] != sectorRoot {
		t.Fatal("reported sector root does not match actual sector root")
	}

	var sectorBuf bytes.Buffer
	err = renter.Read(&sectorBuf, []renterhost.RPCReadRequestSection{{
		MerkleRoot: sectorRoot,
		Offset:     0,
		Length:     renterhost.SectorSize,
	}})
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(sectorBuf.Bytes(), sector[:]) {
		t.Fatal("downloaded sector does not match uploaded sector")
	}

	err = renter.Unlock()
	if err != nil {
		t.Fatal(err)
	}
}

func BenchmarkWrite(b *testing.B) {
	renter, host := createTestingPair(b)
	defer renter.Close()
	defer host.Close()

	sector := [renterhost.SectorSize]byte{0: 1}

	b.ResetTimer()
	b.ReportAllocs()
	b.SetBytes(renterhost.SectorSize)

	for i := 0; i < b.N; i++ {
		_, err := renter.Append(&sector)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkRead(b *testing.B) {
	renter, host := createTestingPair(b)
	defer renter.Close()
	defer host.Close()

	sector := [renterhost.SectorSize]byte{0: 1}
	sectorRoot, err := renter.Append(&sector)
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	b.ReportAllocs()
	b.SetBytes(renterhost.SectorSize)

	for i := 0; i < b.N; i++ {
		err = renter.Read(ioutil.Discard, []renterhost.RPCReadRequestSection{{
			MerkleRoot: sectorRoot,
			Offset:     0,
			Length:     renterhost.SectorSize,
		}})
		if err != nil {
			b.Fatal(err)
		}
	}
}
