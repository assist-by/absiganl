package calculate

// EMA 계산 float 반환
func CalculateEMA(prices []float64, period int) float64 {
	k := 2.0 / float64(period+1)
	ema := prices[0]
	for i := 1; i < len(prices); i++ {
		ema = prices[i]*k + ema*(1-k)
	}
	return ema
}

// EMA 계산 slice 반환
func CalculateEMASlice(prices []float64, period int) []float64 {
	k := 2.0 / float64(period+1)
	ema := make([]float64, len(prices))
	ema[0] = prices[0]
	for i := 1; i < len(prices); i++ {
		ema[i] = prices[i]*k + ema[i-1]*(1-k)
	}
	return ema
}
