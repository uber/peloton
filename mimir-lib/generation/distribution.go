// @generated AUTO GENERATED - DO NOT EDIT!
// Copyright (c) 2017 Uber Technologies, Inc.
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
	"time"
)

// Distribution represents a probability distribution which can change according to time.
type Distribution interface {
	Value(random *rand.Rand, time time.Time) float64
}

// NewConstantGaussian creates a new GaussianDistribution with constant mean and standard deviation.
func NewConstantGaussian(mean, deviation float64) *Gaussian {
	return &Gaussian{
		Mean:              func(time time.Time) float64 { return mean },
		StandardDeviation: func(time time.Time) float64 { return deviation },
	}
}

// Gaussian represents a gaussian distribution with varying mean and standard deviation throughout time.
type Gaussian struct {
	Mean              func(time time.Time) float64
	StandardDeviation func(time time.Time) float64
}

// Value returns a random value from a gaussian with a mean and standard deviation that varies throughout time.
func (gaussian *Gaussian) Value(random *rand.Rand, time time.Time) float64 {
	return random.NormFloat64()*gaussian.StandardDeviation(time) + gaussian.Mean(time)
}

// Discrete represents a discrete distribution with varying probabilities for each value throughout time.
type Discrete struct {
	ValuesToProbabilities map[float64]func(time time.Time) float64
}

// Value returns a random value from a discrete distribution that varies throughout time.
func (discrete *Discrete) Value(random *rand.Rand, time time.Time) float64 {
	keys := make([]float64, len(discrete.ValuesToProbabilities))
	i := 0
	for key := range discrete.ValuesToProbabilities {
		keys[i] = key
		i++
	}
	sort.Float64s(keys)
	sample := random.Float64()
	sum := 0.0
	result := 0.0
	for _, value := range keys {
		probability := discrete.ValuesToProbabilities[value]
		result = value
		sum += probability(time)
		if sample < sum {
			break
		}
	}
	return result
}

// NewUniformDiscrete creates a new Discrete which returns a set of values with a uniform probability
// distribution throughout time.
func NewUniformDiscrete(values ...float64) *Discrete {
	valuesToProbabilities := map[float64]func(time.Time) float64{}
	for _, value := range values {
		valuesToProbabilities[value] = func(time time.Time) float64 {
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
	v := map[float64]func(time.Time) float64{}
	weights := 0.0
	for _, weight := range valueToWeight {
		weights += weight
	}
	for value, weight := range valueToWeight {
		v[value] = func(time time.Time) float64 {
			return weight / weights
		}
	}
	return &Discrete{
		ValuesToProbabilities: v,
	}
}
