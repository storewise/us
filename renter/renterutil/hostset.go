package renterutil

import (
	"context"
	"log"
	"net"
	"strings"
	"time"

	"github.com/pkg/errors"
	"gitlab.com/NebulousLabs/Sia/types"
	"go.uber.org/multierr"
	"golang.org/x/sync/semaphore"

	"lukechampine.com/us/hostdb"
	"lukechampine.com/us/renter"
	"lukechampine.com/us/renter/proto"
)

var (
	ErrNoHost        = errors.New("no record of that host")
	ErrHostAcquired  = errors.New("host is currently acquired")
	ErrHostSetClosed = errors.New("host set closed")
)

// A HostError associates an error with a given host.
type HostError struct {
	HostKey hostdb.HostPublicKey
	Err     error
}

// Error implements error.
func (he HostError) Error() string {
	return he.HostKey.ShortKey() + ": " + he.Err.Error()
}

// Unwrap returns the underlying error.
func (he HostError) Unwrap() error {
	return he.Err
}

// A HostErrorSet is a collection of errors from various hosts.
type HostErrorSet []*HostError

// Error implements error.
func (hes HostErrorSet) Error() string {
	strs := make([]string, len(hes))
	for i := range strs {
		strs[i] = hes[i].Error()
	}
	// include a leading newline so that the first error isn't printed on the
	// same line as the error context
	return "\n" + strings.Join(strs, "\n")
}

func (hse HostErrorSet) Is(target error) bool {
	for _, e := range hse {
		if errors.Is(e, target) {
			return true
		}
	}
	return false
}

type lockedHost struct {
	reconnect func() error
	s         *proto.Session
	mu        *semaphore.Weighted
}

// A HostSet is a collection of renter-host protocol sessions.
type HostSet struct {
	sessions      map[hostdb.HostPublicKey]*lockedHost
	hkr           renter.HostKeyResolver
	currentHeight types.BlockHeight
	lockTimeout   time.Duration
	onConnect     func(s *proto.Session)
}

// HasHost returns true if the specified host is in the set.
func (set *HostSet) HasHost(hostKey hostdb.HostPublicKey) bool {
	_, ok := set.sessions[hostKey]
	return ok
}

func reconnectAfterClose() error { return ErrHostSetClosed }

// Close closes all of the sessions in the set.
func (set *HostSet) Close() error {
	var err error
	for hostKey, lh := range set.sessions {
		if multierr.AppendInto(&err, lh.mu.Acquire(context.Background(), 1)) {
			continue
		}
		if lh.s != nil {
			if e := lh.s.Close(); e != nil && !errors.Is(e, net.ErrClosed) {
				err = multierr.Append(err, e)
			}
			lh.s = nil
		}
		lh.reconnect = reconnectAfterClose
		delete(set.sessions, hostKey)
		lh.mu.Release(1)
	}
	return err
}

func (set *HostSet) acquire(ctx context.Context, host hostdb.HostPublicKey) (*proto.Session, error) {
	ls, ok := set.sessions[host]
	if !ok {
		return nil, ErrNoHost
	}
	if err := ls.mu.Acquire(ctx, 1); err != nil {
		return nil, err
	}
	if err := ls.reconnect(); err != nil {
		ls.mu.Release(1)
		return nil, err
	}
	return ls.s, nil
}

func (set *HostSet) tryAcquire(host hostdb.HostPublicKey) (*proto.Session, error) {
	ls, ok := set.sessions[host]
	if !ok {
		return nil, ErrNoHost
	}
	if !ls.mu.TryAcquire(1) {
		return nil, ErrHostAcquired
	}
	if err := ls.reconnect(); err != nil {
		ls.mu.Release(1)
		return nil, err
	}
	return ls.s, nil
}

func (set *HostSet) release(host hostdb.HostPublicKey) {
	lh := set.sessions[host]
	if lh.s.IsClosed() {
		lh.s = nil // force a reconnect
	}
	lh.mu.Release(1)
}

// SetLockTimeout sets the timeout used for all Lock RPCs in Sessions initiated
// by the HostSet.
func (set *HostSet) SetLockTimeout(timeout time.Duration) { set.lockTimeout = timeout }

// SetOnConnect sets the function called on all newly-connected Sessions.
func (set *HostSet) SetOnConnect(fn func(*proto.Session)) { set.onConnect = fn }

// AddHost adds a host to the set for later use.
func (set *HostSet) AddHost(c renter.Contract) {
	lh := &lockedHost{
		mu: semaphore.NewWeighted(1),
	}
	// lazy connection function
	var lastSeen time.Time
	lh.reconnect = func() error {
		if lh.s != nil && !lh.s.IsClosed() {
			// if it hasn't been long since the last reconnect, assume the
			// connection is still open
			if time.Since(lastSeen) < 2*time.Minute {
				lastSeen = time.Now()
				return nil
			}
			// otherwise, the connection *might* still be open; test by sending
			// a "ping" RPC
			//
			// NOTE: this is somewhat inefficient; it means we might incur an
			// extra roundtrip when we don't need to. Better would be for the
			// caller to handle the reconnection logic after calling whatever
			// RPC it wants to call; that way, we only do extra work if the host
			// has actually disconnected. But that feels too burdensome.
			if _, err := lh.s.Settings(); err == nil {
				lastSeen = time.Now()
				return nil
			}
			// connection timed out, or some other error occurred; close our
			// end (just in case) and fallthrough to the reconnection logic
			if err := lh.s.Close(); err != nil {
				log.Println("failed to close a session:", err)
			}
		}
		hostIP, err := set.hkr.ResolveHostKey(c.HostKey)
		if err != nil {
			return errors.Wrap(err, "could not resolve host key")
		}
		// create and lock the session manually so that we can use our custom
		// lock timeout
		lh.s, err = proto.NewUnlockedSession(hostIP, c.HostKey, set.currentHeight)
		if err != nil {
			return err
		}
		if err := lh.s.Lock(c.ID, c.RenterKey, set.lockTimeout); err != nil {
			err = multierr.Append(err, lh.s.Close())
			return err
		} else if _, err := lh.s.Settings(); err != nil {
			err = multierr.Append(err, lh.s.Close())
			return err
		}
		set.onConnect(lh.s)
		lastSeen = time.Now()
		return nil
	}
	set.sessions[c.HostKey] = lh
}

// RemoveHost removes a host from the set, closing its Session if active.
func (set *HostSet) RemoveHost(host hostdb.HostPublicKey) {
	lh, ok := set.sessions[host]
	if !ok {
		return
	}
	if err := lh.s.Close(); err != nil {
		log.Println("failed to close a session:", err)
	}
	delete(set.sessions, host)
}

// NewHostSet creates an empty HostSet using the provided resolver and current
// height.
func NewHostSet(hkr renter.HostKeyResolver, currentHeight types.BlockHeight) *HostSet {
	return &HostSet{
		hkr:           hkr,
		currentHeight: currentHeight,
		sessions:      make(map[hostdb.HostPublicKey]*lockedHost),
		lockTimeout:   10 * time.Second,
		onConnect:     func(*proto.Session) {},
	}
}
