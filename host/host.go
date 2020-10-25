// Package host implements a Sia hosting framework.
package host

import (
	"crypto/ed25519"
	"time"

	"gitlab.com/NebulousLabs/Sia/crypto"
	"gitlab.com/NebulousLabs/Sia/modules"
	"gitlab.com/NebulousLabs/Sia/types"
	"lukechampine.com/us/hostdb"
	"lukechampine.com/us/renterhost"
)

// A ContractStore stores file contracts, along with some chain metadata.
type ContractStore interface {
	// SigningKey returns the private key used to sign contract revisions.
	SigningKey() ed25519.PrivateKey
	// Contract returns the contract with the specified ID.
	Contract(id types.FileContractID) (Contract, error)
	// AddContract stores the provided contract, overwriting any previous
	// contract with the same ID.
	AddContract(c Contract) error
	// ReviseContract updates the current revision associated with a contract.
	ReviseContract(rev types.FileContractRevision, renterSig, hostSig []byte) error
	// ActionableContracts returns all of the store's contracts for which
	// ContractIsActionable returns true (as of the current block height).
	ActionableContracts() ([]Contract, error)
	// ApplyConsensusChange integrates a ProcessedConsensusChange into the
	// store.
	ApplyConsensusChange(reverted, applied ProcessedConsensusChange, ccid modules.ConsensusChangeID) error
	// ConsensusChangeID returns the ID of the last ProcessedConsensusChange
	// that was integrated by the store.
	ConsensusChangeID() modules.ConsensusChangeID
	// Height returns the current block height.
	Height() types.BlockHeight
}

// A SectorStore stores contract sector data.
type SectorStore interface {
	AddSector(root crypto.Hash, sector *[renterhost.SectorSize]byte) error
	ContractRoots(id types.FileContractID) ([]crypto.Hash, error)
	DeleteSector(root crypto.Hash) error
	Sector(root crypto.Hash) (*[renterhost.SectorSize]byte, error)
	SetContractRoots(id types.FileContractID, roots []crypto.Hash) error
}

// A Wallet provides addresses and funds and signs transactions.
type Wallet interface {
	Address() (types.UnlockHash, error)
	FundTransaction(txn *types.Transaction, cost types.Currency) ([]crypto.Hash, error)
	SignTransaction(txn *types.Transaction, toSign []crypto.Hash) error
}

// A SettingsReporter returns the host's current settings.
type SettingsReporter interface {
	Settings() hostdb.HostSettings
}

// A TransactionPool broadcasts transaction sets to miners for inclusion in an
// upcoming block.
type TransactionPool interface {
	AcceptTransactionSet(txns []types.Transaction) error
	FeeEstimate() (min, max types.Currency, err error)
}

// A Contract is a file contract paired with various metadata.
type Contract struct {
	Revision   types.FileContractRevision
	Signatures [2]types.TransactionSignature

	FormationSet    []types.Transaction
	FinalizationSet []types.Transaction
	ProofSet        []types.Transaction

	FormationConfirmed    bool
	FinalizationConfirmed bool
	ProofConfirmed        bool

	FormationHeight    types.BlockHeight
	FinalizationHeight types.BlockHeight
	ProofHeight        types.BlockHeight
	ProofSegment       uint64

	// Non-nil, with explanatory error message, if it is no longer possible to
	// submit a valid storage proof for the Contract.
	FatalError error
}

// ID returns the contract's ID.
func (c *Contract) ID() types.FileContractID {
	return c.Revision.ParentID
}

// RenterKey returns the renter's public key.
func (c *Contract) RenterKey() types.SiaPublicKey {
	return c.Revision.UnlockConditions.PublicKeys[0]
}

// A MetricsRecorder records various metrics relating to a renter-host protocol
// session.
type MetricsRecorder interface {
	RecordSessionMetric(ctx *SessionContext, m Metric)
}

// SessionContext contains various metadata relating to a renter-host protocol
// session.
type SessionContext struct {
	UID         [16]byte
	RenterIP    string
	Timestamp   time.Time
	Elapsed     time.Duration
	BlockHeight types.BlockHeight
	UpBytes     uint64
	DownBytes   uint64

	Contract types.FileContractRevision
	Settings hostdb.HostSettings
}

// A Metric contains metadata relating to a session event, such as the
// completion of the initial handshake or the initiation of an RPC.
type Metric interface {
	isMetric()
}

func (MetricHandshake) isMetric()  {}
func (MetricSessionEnd) isMetric() {}
func (MetricRPCStart) isMetric()   {}
func (MetricRPCEnd) isMetric()     {}

// MetricHandshake is recorded upon completion of the renter-host protocol
// handshake.
type MetricHandshake struct {
	Err error
}

// MetricSessionEnd is recorded upon termination of the session.
type MetricSessionEnd struct {
	Err error
}

// MetricRPCStart is recorded upon initiation of an RPC.
type MetricRPCStart struct {
	ID        renterhost.Specifier
	Timestamp time.Time
}

// MetricRPCEnd is recorded upon completion of an RPC.
type MetricRPCEnd struct {
	ID        renterhost.Specifier
	Elapsed   time.Duration
	UpBytes   uint64
	DownBytes uint64
	Err       error
}
