// Package merkle provides Sia-specific Merkle tree utilities.
package merkle

import (
	"bytes"
	"sync"

	"gitlab.com/NebulousLabs/Sia/crypto"
	"golang.org/x/crypto/blake2b"
	"lukechampine.com/us/renterhost"
)

const (
	// SegmentSize is the number of bytes in each leaf node of a sector's Merkle
	// tree.
	SegmentSize = crypto.HashSize * 2

	// SegmentsPerSector is a convenience value.
	SegmentsPerSector = renterhost.SectorSize / SegmentSize

	// prefixes used during hashing, as specified by RFC 6962
	leafHashPrefix = 0
	nodeHashPrefix = 1
)

// Much of this code assumes that renterhost.SectorSize is a power of 2; verify this assumption at compile time
var _ [0]struct{} = [renterhost.SectorSize & (renterhost.SectorSize - 1)]struct{}{}

// hashBuffer is a helper type for calculating hashes within a Merkle tree.
type hashBuffer [1 + SegmentSize]byte

func (buf *hashBuffer) leafHash(segment []byte) crypto.Hash {
	if len(segment) != SegmentSize {
		panic("leafHash: illegal input size")
	}
	buf[0] = leafHashPrefix
	copy(buf[1:], segment)
	return crypto.Hash(blake2b.Sum256(buf[:]))
}

func (buf *hashBuffer) nodeHash(left, right crypto.Hash) crypto.Hash {
	buf[0] = nodeHashPrefix
	copy(buf[1:], left[:])
	copy(buf[1+len(left):], right[:])
	return crypto.Hash(blake2b.Sum256(buf[:]))
}

// SectorRoot computes the Merkle root of a sector, using the standard Sia
// leaf size.
func SectorRoot(sector *[renterhost.SectorSize]byte) crypto.Hash {
	// maximize parallelism by calculating each subtree on its own CPU.
	// 8 seems like a reasonable default.
	const numSubtrees = 8
	subtrees := make([]crypto.Hash, numSubtrees)
	const rootsPerSubtree = SegmentsPerSector / numSubtrees
	var wg sync.WaitGroup
	wg.Add(numSubtrees)
	for i := range subtrees {
		go func(i int) {
			sectorData := bytes.NewBuffer(sector[i*rootsPerSubtree*SegmentSize:])
			// instead of calculating the full set of segment roots and then
			// merging them all, break the work into smaller pieces. Not only
			// does this reduce total memory footprint, it also prevents a
			// heap allocation, because the full set of segment roots is too
			// large to fit on the stack. 256 seems to be the sweet spot.
			const numSubsubtrees = 256
			subsubtrees := make([]crypto.Hash, numSubsubtrees)
			roots := make([]crypto.Hash, rootsPerSubtree/numSubsubtrees)
			var buf hashBuffer
			for j := range subsubtrees {
				for k := range roots {
					roots[k] = buf.leafHash(sectorData.Next(SegmentSize))
				}
				subsubtrees[j] = cachedRootAlias(roots)
			}
			subtrees[i] = cachedRootAlias(subsubtrees)
			wg.Done()
		}(i)
	}
	wg.Wait()
	return cachedRootAlias(subtrees)
}

// CachedRoot calculates the root of a set of existing Merkle roots.
func CachedRoot(roots []crypto.Hash) crypto.Hash {
	return cachedRootAlias(append([]crypto.Hash(nil), roots...))
}

// cachedRootAlias calculates the root of a set of existing Merkle
// roots, using the memory of roots as scratch space.
func cachedRootAlias(roots []crypto.Hash) crypto.Hash {
	if len(roots) == 0 {
		return crypto.Hash{}
	}

	var buf hashBuffer
	newRoots := roots
	for len(roots) > 1 {
		newRoots = newRoots[:0]
		for i := 0; i < len(roots); i += 2 {
			if i+1 >= len(roots) {
				newRoots = append(newRoots, roots[i])
				break
			}
			newRoots = append(newRoots, buf.nodeHash(roots[i], roots[i+1]))
		}
		roots = newRoots
	}
	return roots[0]
}