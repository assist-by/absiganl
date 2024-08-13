package calculate

/// MACD 계산
func CalculateMACD(prices []float64) (float64, float64) {
	if len(prices) < 26 {
		return 0, 0 // Not enough data
	}

	ema12 := CalculateEMA(prices, 12)
	ema26 := CalculateEMA(prices, 26)
	macdLine := ema12 - ema26

	ema12Slice := CalculateEMASlice(prices, 12)
	ema26Slice := CalculateEMASlice(prices, 26)
	macdSlice := make([]float64, len(prices))
	for i := 0; i < len(prices); i++ {
		macdSlice[i] = ema12Slice[i] - ema26Slice[i]
	}

	signalLine := CalculateEMA(macdSlice, 9)
	return macdLine, signalLine
}
