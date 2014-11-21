package balancer

import (
	"fmt"
	"math/rand"
	"net"
	"sort"
	"sync"

	"github.com/getlantern/golog"
)

var (
	log = golog.LoggerFor("balancer")
)

var (
	emptyDialers = []*Dialer{}
)

type Balancer struct {
	dialers []*Dialer
	mutex   sync.RWMutex
}

type Dialer struct {
	Weight int
	QOS    int
	Dial   func(network, addr string) (net.Conn, error)
}

func New() *Balancer {
	return &Balancer{
		dialers: make([]*Dialer, 0),
	}
}

func (b *Balancer) Dial(network, addr string, targetQOS int) (net.Conn, error) {
	dialers := b.getDialers()
	for {
		if len(dialers) == 0 {
			return nil, fmt.Errorf("No dialers left to try")
		}
		var dialer *Dialer
		dialer, dialers = randomDialer(dialers, targetQOS)
		conn, err := dialer.Dial(network, addr)
		if err != nil {
			log.Tracef("Unable to dial: %s", err)
			continue
		}
		return conn, nil
	}
}

func (b *Balancer) Add(dialer *Dialer) {
	b.mutex.Lock()
	defer b.mutex.Unlock()
	b.dialers = append(b.dialers, dialer)
}

func (b *Balancer) Remove(dialer *Dialer) {
	b.mutex.Lock()
	defer b.mutex.Unlock()
	b.dialers = withoutDialer(b.dialers, dialer)
}

func (b *Balancer) getDialers() []*Dialer {
	b.mutex.RLock()
	defer b.mutex.RUnlock()
	result := make([]*Dialer, len(b.dialers))
	copy(result, b.dialers)
	return result
}

func randomDialer(dialers []*Dialer, targetQOS int) (chosen *Dialer, others []*Dialer) {
	dialersToTry := dialers
	if targetQOS > 0 {
		// Weed out dialers with too low QOS, preferring higher QOS
		sort.Sort(ByQOS(dialers))
		dialersToTry = make([]*Dialer, 0)
		for i, dialer := range dialers {
			if dialer.QOS >= targetQOS {
				log.Tracef("Including dialer with QOS %d meeting targetQOS %d", dialer.QOS, targetQOS)
				dialersToTry = append(dialersToTry, dialer)
			} else if i == len(dialers)-1 && len(dialersToTry) == 0 {
				log.Trace("No dialers meet targetQOS, using highest QOS dialer of remaining")
				dialersToTry = append(dialersToTry, dialer)
			}
		}
	}

	totalWeights := 0
	for _, dialer := range dialersToTry {
		totalWeights = totalWeights + dialer.Weight
	}

	// Pick a random server using a target value between 0 and the total weights
	t := rand.Intn(totalWeights)
	aw := 0
	for _, dialer := range dialersToTry {
		aw = aw + dialer.Weight
		if aw > t {
			log.Trace("Reached random target value, using this dialer")
			return dialer, withoutDialer(dialers, dialer)
		}
	}

	// We should never reach this
	panic("No dialer found!")
}

func withoutDialer(dialers []*Dialer, dialer *Dialer) []*Dialer {
	for i, existing := range dialers {
		if existing == dialer {
			return without(dialers, i)
		}
	}
	log.Tracef("Dialer not found for removal: %s", dialer)
	return dialers
}

func without(dialers []*Dialer, i int) []*Dialer {
	if len(dialers) == 1 {
		return emptyDialers
	} else if i == len(dialers)-1 {
		return dialers[:i]
	} else {
		return append(dialers[:i], dialers[i+1:]...)
	}
}

// ByQOS implements sort.Interface for []*Dialer based on the QOS
type ByQOS []*Dialer

func (a ByQOS) Len() int           { return len(a) }
func (a ByQOS) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByQOS) Less(i, j int) bool { return a[i].QOS < a[j].QOS }
