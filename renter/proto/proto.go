// Package proto implements the renter side of the Sia renter-host protocol.
package proto // import "lukechampine.com/us/renter/proto"

import (
	"github.com/pkg/errors"
	"gitlab.com/NebulousLabs/Sia/crypto"
	"gitlab.com/NebulousLabs/Sia/modules"
	"gitlab.com/NebulousLabs/Sia/types"
	"lukechampine.com/us/ed25519"
	"lukechampine.com/us/hostdb"
	"lukechampine.com/us/renterhost"
)

func wrapErr(err *error, fnName string) {
	*err = errors.Wrap(*err, fnName)
}

type (
	// A Wallet provides addresses and outputs, and can sign transactions.
	Wallet interface {
		NewWalletAddress() (types.UnlockHash, error)
		SignTransaction(txn *types.Transaction, toSign []crypto.Hash) error
		UnspentOutputs() ([]modules.UnspentOutput, error)
		UnconfirmedParents(txn types.Transaction) ([]types.Transaction, error)
		UnlockConditions(addr types.UnlockHash) (types.UnlockConditions, error)
	}
	// A TransactionPool can broadcast transactions and estimate transaction
	// fees.
	TransactionPool interface {
		AcceptTransactionSet([]types.Transaction) error
		FeeEstimate() (min types.Currency, max types.Currency, err error)
	}
)

// A ContractEditor provides an interface for viewing and updating a file
// contract transaction and the Merkle roots of each sector covered by the
// contract.
type ContractEditor interface {
	// Revision returns the latest revision of the file contract.
	Revision() ContractRevision

	// SetRevision sets the current revision of the file contract. The revision
	// signatures do not need to be verified.
	SetRevision(rev ContractRevision) error

	// Key returns the renter's signing key.
	Key() ed25519.PrivateKey
}

// A ContractRevision contains the most recent revision to a file contract and
// its signatures.
type ContractRevision struct {
	Revision   types.FileContractRevision
	Signatures [2]types.TransactionSignature
}

// EndHeight returns the height at which the host is no longer obligated to
// store contract data.
func (c ContractRevision) EndHeight() types.BlockHeight {
	return c.Revision.NewWindowStart
}

// ID returns the ID of the original FileContract.
func (c ContractRevision) ID() types.FileContractID {
	return c.Revision.ParentID
}

// HostKey returns the public key of the host.
func (c ContractRevision) HostKey() hostdb.HostPublicKey {
	return hostdb.HostKeyFromSiaPublicKey(c.Revision.UnlockConditions.PublicKeys[1])
}

// RenterFunds returns the funds remaining in the contract's Renter payout.
func (c ContractRevision) RenterFunds() types.Currency {
	return c.Revision.NewValidProofOutputs[0].Value
}

// NumSectors returns the number of sectors covered by the contract.
func (c ContractRevision) NumSectors() int {
	return int(c.Revision.NewFileSize / renterhost.SectorSize)
}

// IsValid returns false if the ContractRevision has the wrong number of
// public keys or outputs.
func (c ContractRevision) IsValid() bool {
	return len(c.Revision.NewValidProofOutputs) > 0 &&
		len(c.Revision.UnlockConditions.PublicKeys) == 2
}

// SubmitContractRevision submits the latest revision of a contract to the
// blockchain, finalizing the renter and host payouts as they stand in the
// revision. Submitting a revision with a higher revision number will replace
// the previously-submitted revision.
//
// Submitting revision transactions is a way for the renter to punish the
// host. If the host is well-behaved, there is no incentive for the renter to
// submit revision transactions. But if the host misbehaves, submitting the
// revision ensures that the host will lose the collateral it committed.
func SubmitContractRevision(c ContractRevision, w Wallet, tpool TransactionPool) (err error) {
	defer wrapErr(&err, "SubmitContractRevision")
	// construct a transaction containing the signed revision
	txn := types.Transaction{
		FileContractRevisions: []types.FileContractRevision{c.Revision},
		TransactionSignatures: c.Signatures[:],
	}

	// add the transaction fee
	_, maxFee, err := tpool.FeeEstimate()
	if err != nil {
		return errors.Wrap(err, "could not estimate transaction fee")
	}
	fee := maxFee.Mul64(estTxnSize)
	txn.MinerFees = append(txn.MinerFees, fee)

	// pay for the fee by adding outputs and signing them
	changeAddr, err := w.NewWalletAddress()
	if err != nil {
		return errors.Wrap(err, "could not get a change address to use")
	}
	toSign, err := fundSiacoins(&txn, fee, changeAddr, w)
	if err != nil {
		return err
	}
	if err := w.SignTransaction(&txn, toSign); err != nil {
		return errors.Wrap(err, "failed to sign transaction")
	}

	// submit the funded and signed transaction
	if err := tpool.AcceptTransactionSet([]types.Transaction{txn}); err != nil {
		return err
	}
	return nil
}
