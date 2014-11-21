package balancer

import (
	"fmt"
	"math/rand"
	"net"
	"sort"

	"github.com/getlantern/golog"
)

var (
	log = golog.LoggerFor("balancer")
)

var (
	emptyDialers = []*dialer{}
)

type Balancer struct {
	dialers []*dialer
}

type Dialer struct {
	Weight int
	QOS    int
	Dial   func(network, addr string) (net.Conn, error)
}

type dialer struct {
	*Dialer
}

func New(dialers ...*Dialer) *Balancer {
	dhs := make([]*dialer, 0, len(dialers))
	for _, d := range dialers {
		dhs = append(dhs, &dialer{Dialer: d})
	}
	return &Balancer{
		dialers: dhs,
	}
}

func (b *Balancer) Dial(network, addr string, targetQOS int) (net.Conn, error) {
	dialers := b.getDialers()
	for {
		if len(dialers) == 0 {
			return nil, fmt.Errorf("No dialers left to try")
		}
		var d *dialer
		d, dialers = randomDialer(dialers, targetQOS)
		conn, err := d.Dial(network, addr)
		if err != nil {
			log.Tracef("Unable to dial: %s", err)
			continue
		}
		return conn, nil
	}
}

func (b *Balancer) getDialers() []*dialer {
	result := make([]*dialer, len(b.dialers))
	copy(result, b.dialers)
	return result
}

func randomDialer(dialers []*dialer, targetQOS int) (chosen *dialer, others []*dialer) {
	dialersToTry := dialers
	if targetQOS > 0 {
		// Weed out dialers with too low QOS, preferring higher QOS
		sort.Sort(ByQOS(dialers))
		dialersToTry = make([]*dialer, 0)
		for i, d := range dialers {
			if d.QOS >= targetQOS {
				log.Tracef("Including dialer with QOS %d meeting targetQOS %d", d.QOS, targetQOS)
				dialersToTry = append(dialersToTry, d)
			} else if i == len(dialers)-1 && len(dialersToTry) == 0 {
				log.Trace("No dialers meet targetQOS, using highest QOS dialer of remaining")
				dialersToTry = append(dialersToTry, d)
			}
		}
	}

	totalWeights := 0
	for _, d := range dialersToTry {
		totalWeights = totalWeights + d.Weight
	}

	// Pick a random server using a target value between 0 and the total weights
	t := rand.Intn(totalWeights)
	aw := 0
	for _, d := range dialersToTry {
		aw = aw + d.Weight
		if aw > t {
			log.Trace("Reached random target value, using this dialer")
			return d, withoutDialer(dialers, d)
		}
	}

	// We should never reach this
	panic("No dialer found!")
}

func withoutDialer(dialers []*dialer, d *dialer) []*dialer {
	for i, existing := range dialers {
		if existing == d {
			return without(dialers, i)
		}
	}
	log.Tracef("Dialer not found for removal: %s", d)
	return dialers
}

func without(dialers []*dialer, i int) []*dialer {
	if len(dialers) == 1 {
		return emptyDialers
	} else if i == len(dialers)-1 {
		return dialers[:i]
	} else {
		return append(dialers[:i], dialers[i+1:]...)
	}
}

// ByQOS implements sort.Interface for []*dialer based on the QOS
type ByQOS []*dialer

func (a ByQOS) Len() int           { return len(a) }
func (a ByQOS) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByQOS) Less(i, j int) bool { return a[i].QOS < a[j].QOS }
