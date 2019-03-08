// @generated AUTO GENERATED - DO NOT EDIT! 117d51fa2854b0184adc875246a35929bbbf0a91

// Copyright (c) 2018 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package generation

import (
	"math/rand"
	"sort"
	"sync"
	"time"
)

// Distribution represents a probability distribution which can change according to time but always returns the same
// value for the same point in time.
type Distribution interface {
	Value(random Random, gtime time.Duration) float64
}

// Random is source of randomness that varies throughout time, but will always return the same value for the same
// instance in time.
type Random interface {
	// Uniform returns a value in the range [0;1[.
	Uniform(gtime time.Duration) float64

	// Norm returns a normal distributed value in the range [-math.MaxFloat64, +math.MaxFloat64] with mean 0.0 and
	// stddev 1.0.
	Norm(gtime time.Duration) float64

	// Exp returns a exponentially distributed value in the range [0, +math.MaxFloat64] with an exponential distribution
	// whose rate parameter (lambda) is 1 and whose mean is 1/lambda (1).
	Exp(gtime time.Duration) float64

	// Perm returns permutation of the numbers 0, 1, ..., n-1.
	Perm(gtime time.Duration, n int) []int
}

// NewRandom returns a new source of randomness with the given seed.
func NewRandom(seed int64) Random {
	return &randomImpl{
		source: rand.New(rand.NewSource(seed)),
		seed:   seed,
	}
}

type randomImpl struct {
	source *rand.Rand
	seed   int64
	lock   sync.Mutex
}

func (random *randomImpl) Uniform(gtime time.Duration) float64 {
	random.lock.Lock()
	defer random.lock.Unlock()
	random.source.Seed(random.seed + int64(gtime))
	return random.source.Float64()
}

func (random *randomImpl) Norm(gtime time.Duration) float64 {
	random.lock.Lock()
	defer random.lock.Unlock()
	random.source.Seed(random.seed + int64(gtime))
	return random.source.NormFloat64()
}

func (random *randomImpl) Exp(gtime time.Duration) float64 {
	random.lock.Lock()
	defer random.lock.Unlock()
	random.source.Seed(random.seed + int64(gtime))
	return random.source.ExpFloat64()
}

func (random *randomImpl) Perm(gtime time.Duration, n int) []int {
	random.lock.Lock()
	defer random.lock.Unlock()
	random.source.Seed(random.seed + int64(gtime))
	return random.source.Perm(n)
}

// NewConstantGaussian creates a new GaussianDistribution with constant mean and standard deviation.
func NewConstantGaussian(mean, deviation float64) *Gaussian {
	return &Gaussian{
		Mean:              func(gtime time.Duration) float64 { return mean },
		StandardDeviation: func(gtime time.Duration) float64 { return deviation },
	}
}

// Gaussian represents a gaussian distribution with varying mean and standard deviation throughout time.
type Gaussian struct {
	Mean              func(gtime time.Duration) float64
	StandardDeviation func(gtime time.Duration) float64
}

// Value returns a random value from a gaussian with a mean and standard deviation that varies throughout time.
func (gaussian *Gaussian) Value(random Random, gtime time.Duration) float64 {
	return random.Norm(gtime)*gaussian.StandardDeviation(gtime) + gaussian.Mean(gtime)
}

// Discrete represents a discrete distribution with varying probabilities for each value throughout time.
type Discrete struct {
	ValuesToProbabilities map[float64]func(gtime time.Duration) float64
}

// Value returns a random value from a discrete distribution that varies throughout time.
func (discrete *Discrete) Value(random Random, gtime time.Duration) float64 {
	keys := make([]float64, len(discrete.ValuesToProbabilities))
	i := 0
	for key := range discrete.ValuesToProbabilities {
		keys[i] = key
		i++
	}
	sort.Float64s(keys)
	sample := random.Uniform(gtime)
	sum := 0.0
	result := 0.0
	for _, value := range keys {
		probability := discrete.ValuesToProbabilities[value]
		result = value
		sum += probability(gtime)
		if sample < sum {
			break
		}
	}
	return result
}

// NewUniformDiscrete creates a new Discrete which returns a set of values with a uniform probability
// distribution throughout time.
func NewUniformDiscrete(values ...float64) *Discrete {
	valuesToProbabilities := map[float64]func(time.Duration) float64{}
	for _, value := range values {
		valuesToProbabilities[value] = func(gtime time.Duration) float64 {
			return 1.0 / float64(len(values))
		}
	}
	return &Discrete{
		ValuesToProbabilities: valuesToProbabilities,
	}
}

// NewDiscrete creates a new discrete distribution which returns each value with a probability distributed that
// corresponds to the weight of the value.
func NewDiscrete(valueToWeight map[float64]float64) *Discrete {
	v := map[float64]func(time.Duration) float64{}
	weights := 0.0
	for _, weight := range valueToWeight {
		weights += weight
	}
	for value, weight := range valueToWeight {
		v[value] = func(gtime time.Duration) float64 {
			return weight / weights
		}
	}
	return &Discrete{
		ValuesToProbabilities: v,
	}
}

// Constant represents a constant distribution which always returns the same value, though the value to return can be
// changed using the new value method.
type Constant struct {
	value float64
}

// NewConstant creates a new constant distribution.
func NewConstant(value float64) *Constant {
	return &Constant{
		value: value,
	}
}

// Value returns the currently set constant value.
func (constant *Constant) Value(random Random, gtime time.Duration) float64 {
	return constant.value
}

// NewValue changes the value of the constant distribution to a new constant value.
func (constant *Constant) NewValue(value float64) {
	constant.value = value
}

// CurrentValue returns the current value of the constant distribution.
func (constant *Constant) CurrentValue() float64 {
	return constant.value
}
