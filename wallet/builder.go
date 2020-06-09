package wallet

import (
	"crypto/ed25519"
	"math/big"
	"unsafe"

	"gitlab.com/NebulousLabs/Sia/types"
	"lukechampine.com/us/ed25519hash"
)

// BytesPerInput is the encoded size of a SiacoinInput and corresponding
// TransactionSignature, assuming standard UnlockConditions.
const BytesPerInput = 241

// SumOutputs returns the total value of the supplied outputs.
func SumOutputs(outputs []UnspentOutput) types.Currency {
	sum := new(big.Int)
	for _, o := range outputs {
		// sum = sum.Add(o.Value) would allocate a new value for every output;
		// instead, cheat to get a pointer to the underlying big.Int
		c := (*struct {
			i big.Int
		})(unsafe.Pointer(&o.Value))
		sum.Add(sum, &c.i)
	}
	return types.NewCurrency(sum)
}

// FundAtLeast selects a set of inputs whose total value is at least amount,
// returning the selected inputs and the resulting change, or false if the sum
// of all inputs is less than amount.
func FundAtLeast(amount types.Currency, inputs []ValuedInput) (used []ValuedInput, change types.Currency, ok bool) {
	var outputSum types.Currency
	for i, o := range inputs {
		if outputSum = outputSum.Add(o.Value); outputSum.Cmp(amount) >= 0 {
			return inputs[:i+1], outputSum.Sub(amount), true
		}
	}
	return nil, types.ZeroCurrency, amount.IsZero()
}

// FundTransaction selects a set of inputs whose total value is amount+fee,
// where fee is the estimated fee required to pay for the inputs and their
// signatures.
func FundTransaction(amount, feePerByte types.Currency, inputs []ValuedInput) (used []ValuedInput, fee, change types.Currency, ok bool) {
	// we need to fund amount+fee, but the exact fee depends on the number of
	// inputs we use...which depends on the fee. Start by getting the number of
	// inputs required to fund just the amount, then iterate until we find a
	// solution.
	used, change, ok = FundAtLeast(amount, inputs)
	if !ok {
		return nil, types.ZeroCurrency, types.ZeroCurrency, false
	}
	numInputs := len(used)
	for {
		fee = feePerByte.Mul64(BytesPerInput).Mul64(uint64(numInputs))
		used, change, ok = FundAtLeast(amount.Add(fee), inputs)
		if !ok {
			return nil, types.ZeroCurrency, types.ZeroCurrency, false
		} else if len(used) == numInputs {
			// adjusting the fee did not change the number of inputs required, so
			// we are done.
			return used, fee, change, true
		}
		numInputs = len(used)
	}
}

// AppendTransactionSignature appends a TransactionSignature to txn and signs it
// with key.
func AppendTransactionSignature(txn *types.Transaction, txnSig types.TransactionSignature, key ed25519.PrivateKey) {
	txn.TransactionSignatures = append(txn.TransactionSignatures, txnSig)
	sigIndex := len(txn.TransactionSignatures) - 1
	txn.TransactionSignatures[sigIndex].Signature = ed25519hash.Sign(key, txn.SigHash(sigIndex, types.ASICHardforkHeight+1))
}

// UnconfirmedParents returns the parents of txn that are in limbo.
func UnconfirmedParents(txn types.Transaction, limbo []LimboTransaction) []LimboTransaction {
	// first, map each output created in a limbo transaction to its parent
	outputToParent := make(map[types.OutputID]*LimboTransaction)
	for i := range limbo {
		for j := range limbo[i].SiacoinOutputs {
			scoid := limbo[i].SiacoinOutputID(uint64(j))
			outputToParent[types.OutputID(scoid)] = &limbo[i]
		}
		for j := range limbo[i].SiafundOutputs {
			sfoid := limbo[i].SiafundOutputID(uint64(j))
			outputToParent[types.OutputID(sfoid)] = &limbo[i]
		}
	}

	// then, for each input spent in txn, if that input was created by a limbo
	// transaction, add that limbo transaction to the returned set.
	var parents []LimboTransaction
	seen := make(map[types.TransactionID]struct{})
	addParent := func(parent *LimboTransaction) {
		txid := parent.ID()
		if _, ok := seen[txid]; !ok {
			seen[txid] = struct{}{}
			parents = append(parents, *parent)
		}
	}
	for _, sci := range txn.SiacoinInputs {
		if parent, ok := outputToParent[types.OutputID(sci.ParentID)]; ok {
			addParent(parent)
		}
	}
	for _, sfi := range txn.SiafundInputs {
		if parent, ok := outputToParent[types.OutputID(sfi.ParentID)]; ok {
			addParent(parent)
		}
	}
	return parents
}

// DistributeFunds is a helper function for distributing the value in a set of
// inputs among a set of outputs, each containin per siacoins. It returns the
// number of such outputs that can be funded, along with the transaction fee and
// change amount. Note that such a transaction is only worthwhile if numOuts is
// at least 2.
func DistributeFunds(inputs []UnspentOutput, per, feePerByte types.Currency) (numOuts uint64, fee, change types.Currency) {
	total := SumOutputs(inputs)
	fee = feePerByte.Mul64(BytesPerInput).Mul64(uint64(len(inputs)))
	if fee.Cmp(total) >= 0 {
		return 0, types.ZeroCurrency, types.ZeroCurrency
	}
	total = total.Sub(fee)
	numOuts = total.Div(per).Big().Uint64()
	change = total.Sub(per.Mul64(numOuts))
	return numOuts, fee, change
}
