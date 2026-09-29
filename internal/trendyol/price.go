package trendyol

import (
	"math"
	"strings"
)

// PricePolicy converts the page's quoted price to USD before eligibility is
// evaluated. TRYPerUSD is a configuration snapshot so a long crawl remains
// reproducible and does not depend on a live FX service for every request.
type PricePolicy struct {
	MinUSD    float64
	TRYPerUSD float64
}

func (p PricePolicy) USD(price float64, currency string) (float64, bool) {
	if price < 0 || p.TRYPerUSD <= 0 {
		return 0, false
	}
	switch strings.ToUpper(strings.TrimSpace(currency)) {
	case "", "TRY", "TL":
		price /= p.TRYPerUSD
	case "USD", "$":
	default:
		return 0, false
	}
	// Persist exactly two decimals and apply the threshold to that same value,
	// preventing a raw value such as 20.001 from being stored as an invalid 20.00.
	price = math.Round(price*100) / 100
	if price <= p.MinUSD {
		return price, false
	}
	return price, true
}
