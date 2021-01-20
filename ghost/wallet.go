package ghost

import (
	"gitlab.com/NebulousLabs/Sia/crypto"
	"gitlab.com/NebulousLabs/Sia/types"
)

type StubWallet struct{}

func (StubWallet) Address() (_ types.UnlockHash, _ error) { return }
func (StubWallet) FundTransaction(*types.Transaction, types.Currency) ([]crypto.Hash, func(), error) {
	return nil, func() {}, nil
}
func (StubWallet) SignTransaction(txn *types.Transaction, toSign []crypto.Hash) error {
	txn.TransactionSignatures = append(txn.TransactionSignatures, make([]types.TransactionSignature, len(toSign))...)
	return nil
}

type StubTpool struct{}

func (StubTpool) AcceptTransactionSet([]types.Transaction) (_ error)                    { return }
func (StubTpool) UnconfirmedParents(types.Transaction) (_ []types.Transaction, _ error) { return }
func (StubTpool) FeeEstimate() (_, _ types.Currency, _ error)                           { return }
