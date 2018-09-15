package binpacking

import (
	log "github.com/sirupsen/logrus"
)

const (
	// DeFrag is the name for the de-fragmentation policy
	DeFrag = "DEFRAG"

	// FirstFit is the name of the First Fit policy
	FirstFit = "FIRST_FIT"
)

// RankerFunc type of func which returns Ranker interface
type RankerFunc func() Ranker

// map of ranker name to Init Ranker Func
var rankers = make(map[string]RankerFunc)

// Register registers the ranker and keep it in the
// ranker map.
func Register(name string, ranker RankerFunc) {
	log.Infof("Registering %s Ranker", name)
	if ranker == nil {
		log.Errorf("ranker does not exist")
		return
	}
	if _, registered := rankers[name]; registered {
		log.Errorf("ranker already registered")
		return
	}
	rankers[name] = ranker
}

// Init registers all the rankers
func Init() {
	Register(DeFrag, NewDeFragRanker)
	Register(FirstFit, NewFirstFitRanker)
}

// CreateRanker creates and returns the ranker specified
func CreateRanker(name string) Ranker {
	ranker, ok := rankers[name]
	if !ok {
		log.Errorf("Ranker is not registered")
		return nil
	}
	return ranker()
}
