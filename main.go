package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"

	calculate "github.com/Lux-N-Sal/autro-signal/calculate"
	"github.com/segmentio/kafka-go"
)

type CandleData struct {
	OpenTime                 int64
	Open, High, Low, Close   string
	Volume                   string
	CloseTime                int64
	QuoteAssetVolume         string
	NumberOfTrades           int
	TakerBuyBaseAssetVolume  string
	TakerBuyQuoteAssetVolume string
}

type TechnicalIndicators struct {
	EMA200       float64
	MACD         float64
	Signal       float64
	ParabolicSAR float64
}

type SignalCondition struct {
	Condition bool
	Value     float64
}

type SignalConditions struct {
	Long  [3]SignalCondition
	Short [3]SignalCondition
}

type SignalResult struct {
	Signal     string
	Timestamp  int64
	Price      float64
	StopLoss   float64
	TakeProfie float64
	Conditions SignalConditions
}

var (
	kafkaBroker              string
	kafkaTopicFromPrice      string
	kafkaTopicToNotification string
)

func init() {
	kafkaBroker = os.Getenv("KAFKA_BROKER")
	if kafkaBroker == "" {
		kafkaBroker = "kafka:9092"
	}

	kafkaTopicFromPrice = os.Getenv("KAFKA_TOPIC_FROM_PRICE")
	if kafkaTopicFromPrice == "" {
		kafkaTopicFromPrice = "price-to-signal"
	}
	kafkaTopicToNotification = os.Getenv("KAFKA_TOPIC_TO_NOTIFICATION")
	if kafkaTopicToNotification == "" {
		kafkaTopicToNotification = "signal-to-notification"
	}
}

// / consumer와 연결함수
func createReader() *kafka.Reader {
	return kafka.NewReader(kafka.ReaderConfig{
		Brokers:     []string{kafkaBroker},
		Topic:       kafkaTopicFromPrice,
		MaxAttempts: 5,
	})
}

func createWriter() *kafka.Writer {
	return kafka.NewWriter(kafka.WriterConfig{
		Brokers:     []string{kafkaBroker},
		Topic:       kafkaTopicToNotification,
		MaxAttempts: 5,
	})
}

func writeToKafka(writer *kafka.Writer, signalReuslt SignalResult) error {
	value, err := json.Marshal(signalReuslt)
	if err != nil {
		return fmt.Errorf("error marshalling signal result: %v", err)
	}

	err = writer.WriteMessages(context.Background(), kafka.Message{
		Value: value,
	})

	return err
}

// / 보조 지표 계산
func calculateIndicators(candles []CandleData) (TechnicalIndicators, error) {
	if len(candles) < 300 {
		return TechnicalIndicators{}, fmt.Errorf("insufficient data: need at least 300 candles, got %d", len(candles))
	}

	prices := make([]float64, len(candles))
	highs := make([]float64, len(candles))
	lows := make([]float64, len(candles))

	for i, candle := range candles {
		price, err := strconv.ParseFloat(candle.Close, 64)
		if err != nil {
			return TechnicalIndicators{}, fmt.Errorf("error parsing close price: %v", err)
		}
		prices[i] = price

		high, err := strconv.ParseFloat(candle.High, 64)
		if err != nil {
			return TechnicalIndicators{}, fmt.Errorf("error parsing high price: %v", err)
		}
		highs[i] = high

		low, err := strconv.ParseFloat(candle.Low, 64)
		if err != nil {
			return TechnicalIndicators{}, fmt.Errorf("error parsing low price: %v", err)
		}
		lows[i] = low
	}

	ema200 := calculate.CalculateEMA(prices, 200)
	macd, signal := calculate.CalculateMACD(prices)
	parabolicSAR := calculate.CalculateParabolicSAR(highs, lows)

	return TechnicalIndicators{
		EMA200:       ema200,
		MACD:         macd,
		Signal:       signal,
		ParabolicSAR: parabolicSAR,
	}, nil
}

// signal 생성 함수
func generateSignal(candles []CandleData, indicators TechnicalIndicators) (string, SignalConditions, float64, float64) {
	lastPrice, _ := strconv.ParseFloat(candles[len(candles)-1].Close, 64)
	lastHigh, _ := strconv.ParseFloat(candles[len(candles)-1].High, 64)
	lastLow, _ := strconv.ParseFloat(candles[len(candles)-1].Low, 64)

	conditions := SignalConditions{
		Long: [3]SignalCondition{
			{Condition: lastPrice > indicators.EMA200, Value: lastPrice - indicators.EMA200},
			{Condition: indicators.MACD > indicators.Signal, Value: indicators.MACD - indicators.Signal},
			{Condition: indicators.ParabolicSAR < lastLow, Value: lastLow - indicators.ParabolicSAR},
		},
		Short: [3]SignalCondition{
			{Condition: lastPrice < indicators.EMA200, Value: lastPrice - indicators.EMA200},
			{Condition: indicators.MACD < indicators.Signal, Value: indicators.MACD - indicators.Signal},
			{Condition: indicators.ParabolicSAR > lastHigh, Value: indicators.ParabolicSAR - lastHigh},
		},
	}

	var stopLoss, takeProfit float64

	if conditions.Long[0].Condition && conditions.Long[1].Condition && conditions.Long[2].Condition {
		stopLoss = indicators.ParabolicSAR
		takeProfit = lastPrice + (lastPrice - stopLoss)
		return "LONG", conditions, stopLoss, takeProfit
	} else if conditions.Short[0].Condition && conditions.Short[1].Condition && conditions.Short[2].Condition {
		stopLoss = indicators.ParabolicSAR
		takeProfit = lastPrice - (stopLoss - lastPrice)
		return "SHORT", conditions, stopLoss, takeProfit
	}
	return "NO SIGNAL", conditions, 0.0, 0.0
}

func main() {
	log.Println("Starting Signal Service...")

	reader := createReader()
	defer reader.Close()

	writer := createWriter()
	defer writer.Close()

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

	log.Println("Signal Service is now running. Press CTRL-C to exit.")

	for {
		select {
		case <-signals:
			log.Println("Interrupt is detected. Gracefully shutting down...")
			return

		default:
			msg, err := reader.ReadMessage(context.Background())
			if err != nil {
				log.Printf("Error reading message: %v\n", err)
			}

			var candles []CandleData
			if err := json.Unmarshal(msg.Value, &candles); err != nil {
				log.Printf("Error unmarshalling message: %v\n", err)
				continue
			}

			if len(candles) == 300 {
				indicators, err := calculateIndicators(candles)
				if err != nil {
					log.Printf("Error calculating indicators: %v\n", err)
					continue
				}

				signalType, conditions, stopLoss, takeProfit := generateSignal(candles, indicators)
				lastCandle := candles[len(candles)-1]
				price, err := strconv.ParseFloat(lastCandle.Close, 64)
				if err != nil {
					log.Printf("Error convert price to float: %v\n", err)
					continue
				}

				signalResult := SignalResult{
					Signal:     signalType,
					Timestamp:  lastCandle.CloseTime,
					Price:      price,
					Conditions: conditions,
					StopLoss:   stopLoss,
					TakeProfie: takeProfit,
				}

				err = writeToKafka(writer, signalResult)

				if err != nil {
					log.Printf("Error sending signal to API Gateway: %v", err)
				} else {
					log.Printf("Signal sent to API Gateway. Response: %v", signalResult)
				}
			}
		}
	}
}
