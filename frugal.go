// Package frugal implements a frugal streaming quantile algorithm
/*

This implements Algorithm 3 from
"Frugal Streaming for Estimating Quantiles" (Qiang Ma, S. Muthukrishnan, and Mark Sandler 2013)
( http://arxiv.org/abs/1407.1121 )

For more information, please see http://blog.aggregateknowledge.com/2013/09/16/sketch-of-the-day-frugal-streaming/

*/
package frugal

import (
	"math/rand"
)

// Frugal2U is a frugal stream estimator
type Frugal2U struct {
	m    int     // current estimate
	q    float32 // quantile
	step int
	sign int
	r    *rand.Rand
	f    func(int) int
}

// New constructs a new frugal stream estimator
func New(estimate int, quantile float32) *Frugal2U {
	return &Frugal2U{
		m:    estimate,
		q:    quantile,
		step: 1,
		sign: 0,
		r:    rand.New(rand.NewSource(rand.Int63())),
		f:    func(int) int { return 1 },
	}
}

// Estimate returns the current estimated value for the quantile
func (f2 *Frugal2U) Estimate() int {
	return f2.m
}

// Insert inserts a value into the quantile stream.
func (f2 *Frugal2U) Insert(s int) {

	if f2.sign == 0 {
		// first item is our estimate if we have nothing else
		f2.m = s
		f2.sign = 1
		return
	}

	rnd := f2.r.Float32()

	if s > f2.m && rnd > 1-f2.q {
		f2.step += f2.sign * f2.f(f2.step)
		if f2.step > 0 {
			f2.m += f2.step
		} else {
			f2.m += 1
		}

		if f2.m > s {
			f2.step += (s - f2.m)
			f2.m = s
		}

		if f2.sign < 0 && f2.step > 1 {
			f2.step = 1
		}

		f2.sign = 1

	} else if s < f2.m && rnd > f2.q {
		f2.step += -f2.sign * f2.f(f2.step)
		if f2.step > 0 {
			f2.m -= f2.step
		} else {
			f2.m--
		}

		if f2.m < s {
			f2.step += (f2.m - s)
			f2.m = s
		}

		if f2.sign > 0 && f2.step > 1 {
			f2.step = 1
		}

		f2.sign = -1
	}
}
