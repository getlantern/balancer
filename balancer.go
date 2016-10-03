// Package balancer provides load balancing of network connections per
// different strategies.
package balancer

import (
	"container/heap"
	"fmt"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/getlantern/errors"
	"github.com/getlantern/golog"
	"github.com/getlantern/ops"
)

const (
	dialAttempts = 3

	// reasonable but slightly larger than the timeout of all dialers
	initialTimeout = 1 * time.Minute
)

var (
	// When Dial() is called after an idle period larger than
	// recheckAfterIdleFor, Balancer will recheck all dialers to make sure they
	// are alive and have up-to-date metrics.
	recheckAfterIdleFor = 1 * time.Minute

	log = golog.LoggerFor("balancer")
)

// Balancer balances connections among multiple Dialers.
type Balancer struct {
	// make sure to align on 64bit boundary
	lastDialTime int64 // Time.UnixNano()
	nextTimeout  *emaDuration
	st           Strategy
	mu           sync.RWMutex
	dialers      dialerHeap
	trusted      dialerHeap
}

// New creates a new Balancer using the supplied Strategy and Dialers.
func New(st Strategy, dialers ...*Dialer) *Balancer {
	// a small alpha to gradually adjust timeout based on performance of all
	// dialers
	b := &Balancer{st: st, nextTimeout: newEMADuration(initialTimeout, 0.2)}
	b.Reset(dialers...)
	return b
}

// Reset closes existing dialers and replaces them with new ones.
func (b *Balancer) Reset(dialers ...*Dialer) {
	var dls []*dialer
	var tdls []*dialer

	for _, d := range dialers {
		dl := &dialer{Dialer: d}
		dl.Start()
		dls = append(dls, dl)

		if dl.Trusted {
			tdls = append(tdls, dl)
		}
	}
	b.mu.Lock()
	oldDialers := b.dialers
	b.dialers = b.st(dls)
	b.trusted = b.st(tdls)
	heap.Init(&b.dialers)
	heap.Init(&b.trusted)
	b.mu.Unlock()
	for _, d := range oldDialers.dialers {
		d.Stop()
	}
}

// OnRequest calls Dialer.OnRequest for every dialer in this balancer.
func (b *Balancer) OnRequest(req *http.Request) {
	b.mu.RLock()
	b.dialers.onRequest(req)
	b.mu.RUnlock()
}

// Dial dials (network, addr) using one of the currently active configured
// Dialers. The Dialer to choose depends on the Strategy when creating the
// balancer. Only Trusted Dialers are used to dial HTTP hosts.
//
// If a Dialer fails to connect, Dial will keep trying at most 3 times until it
// either manages to connect, or runs out of dialers in which case it returns an
// error.
func (b *Balancer) Dial(network, addr string) (net.Conn, error) {
	now := time.Now()
	lastDialTime := time.Unix(0, atomic.SwapInt64(&b.lastDialTime, now.UnixNano()))
	idlePeriod := now.Sub(lastDialTime)
	if idlePeriod > recheckAfterIdleFor {
		log.Debugf("Balancer idle for %s, start checking all dialers", idlePeriod)
		b.checkDialers()
	}

	trustedOnly := false
	_, port, _ := net.SplitHostPort(addr)
	// We try to identify HTTP traffic (as opposed to HTTPS) by port and only
	// send HTTP traffic to dialers marked as trusted.
	if port == "" || port == "80" || port == "8080" {
		trustedOnly = true
	}

	var lastDialer *dialer
	for i := 0; i < dialAttempts; i++ {
		d, pickErr := b.pickDialer(trustedOnly)
		if pickErr != nil {
			return nil, pickErr
		}
		if d == lastDialer {
			log.Debugf("Skip dialing %s://%s with same dailer %s", network, addr, d.Label)
			continue
		}
		lastDialer = d
		log.Tracef("Dialing %s://%s with %s", network, addr, d.Label)

		conn, err := b.dialWithTimeout(d, network, addr)
		if err != nil {
			log.Error(errors.New("Unable to dial via %v to %s://%s: %v on pass %v...continuing", d.Label, network, addr, err, i))
			continue
		}
		log.Debugf("Successfully dialed via %v to %v://%v on pass %v", d.Label, network, addr, i)
		return conn, nil
	}
	return nil, fmt.Errorf("Still unable to dial %s://%s after %d attempts", network, addr, dialAttempts)
}

func (b *Balancer) dialWithTimeout(d *dialer, network, addr string) (net.Conn, error) {
	limit := b.nextTimeout.Get()
	timer := time.NewTimer(limit)
	var conn net.Conn
	var err error
	// to synchronize access of conn and err between outer and inner goroutine
	chDone := make(chan bool)
	t := time.Now()
	ops.Go(func() {
		conn, err = d.dial(network, addr)
		if err == nil {
			newTimeout := b.nextTimeout.UpdateWith(3 * time.Since(t))
			log.Tracef("Updated nextTimeout to %v", newTimeout)
		}
		chDone <- true
	})
	for {
		select {
		case _ = <-timer.C:
			// give current dialer a chance to return/fail and other dialers to
			// take part in.
			if d.ConsecSuccesses() > 0 {
				log.Debugf("Reset balancer dial timeout because dialer %s suddenly slows down", d.Label)
				b.nextTimeout.Set(initialTimeout)
				timer.Reset(initialTimeout)
				continue
			}
			// clean up
			ops.Go(func() {
				_ = <-chDone
				if conn != nil {
					_ = conn.Close()
				}
			})
			return nil, errors.New("timeout").With("limit", limit)
		case _ = <-chDone:
			return conn, err
		}
	}
}

// Close closes this Balancer, stopping all background processing. You must call
// Close to avoid leaking goroutines.
func (b *Balancer) Close() {
	b.mu.Lock()
	oldDialers := b.dialers
	b.dialers.dialers = nil
	b.mu.Unlock()
	for _, d := range oldDialers.dialers {
		d.Stop()
	}
}

// Parallel check all dialers
func (b *Balancer) checkDialers() {
	b.mu.RLock()
	for _, d := range b.dialers.dialers {
		go d.check()
	}
	b.mu.RUnlock()
}

func (b *Balancer) pickDialer(trustedOnly bool) (*dialer, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	dialers := &b.dialers
	if trustedOnly {
		dialers = &b.trusted
	}
	if dialers.Len() == 0 {
		if trustedOnly {
			return nil, fmt.Errorf("No trusted dialers")
		}
		return nil, fmt.Errorf("No dialers")
	}
	// heap will re-adjust based on new metrics
	d := heap.Pop(dialers).(*dialer)
	heap.Push(dialers, d)
	return d, nil
}

type dialerHeap struct {
	dialers  []*dialer
	lessFunc func(i, j int) bool
}

func (s *dialerHeap) Len() int { return len(s.dialers) }

func (s *dialerHeap) Swap(i, j int) {
	s.dialers[i], s.dialers[j] = s.dialers[j], s.dialers[i]
}

func (s *dialerHeap) Less(i, j int) bool {
	return s.lessFunc(i, j)
}

func (s *dialerHeap) Push(x interface{}) {
	s.dialers = append(s.dialers, x.(*dialer))
}

func (s *dialerHeap) Pop() interface{} {
	old := s.dialers
	n := len(old)
	x := old[n-1]
	s.dialers = old[0 : n-1]
	return x
}

func (s *dialerHeap) onRequest(req *http.Request) {
	for _, d := range s.dialers {
		if d.OnRequest != nil {
			d.OnRequest(req)
		}
	}
	return
}
