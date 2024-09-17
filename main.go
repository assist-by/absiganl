package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"

	calculate "github.com/assist-by/absignal/calculate"
	lib "github.com/assist-by/autro-library"
	signalType "github.com/assist-by/autro-library/signal_type"
	"github.com/segmentio/kafka-go"
)

var (
	kafkaBroker              string
	kafkaTopicFromPrice      string
	kafkaTopicToNotification string
	registrationTopic        string
	host                     string
	port                     string
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
	registrationTopic = os.Getenv("REGISTRATION_TOPIC")
	if registrationTopic == "" {
		registrationTopic = "service-registration"
	}
	host = os.Getenv("HOST")
	if host == "" {
		host = "autro-signal"
	}
	port = os.Getenv("PORT")
	if port == "" {
		port = "50052"
	}

}

// consumer와 연결함수
func createReader() *kafka.Reader {
	return kafka.NewReader(kafka.ReaderConfig{
		Brokers:     []string{kafkaBroker},
		Topic:       kafkaTopicFromPrice,
		MaxAttempts: 5,
	})
}

// producer 생성
func createWriter() *kafka.Writer {
	return kafka.NewWriter(kafka.WriterConfig{
		Brokers:     []string{kafkaBroker},
		Topic:       kafkaTopicToNotification,
		MaxAttempts: 5,
	})
}

func writeToKafka(writer *kafka.Writer, signalReuslt lib.SignalResult) error {
	value, err := json.Marshal(signalReuslt)
	if err != nil {
		return fmt.Errorf("error marshalling signal result: %v", err)
	}

	err = writer.WriteMessages(context.Background(), kafka.Message{
		Value: value,
	})

	return err
}

// Service Discovery에 등록하는 함수
func registerService(writer *kafka.Writer) error {
	service := lib.Service{
		Name:    "autro-signal",
		Address: fmt.Sprintf("%s:%s", host, port),
	}

	jsonData, err := json.Marshal(service)
	if err != nil {
		return fmt.Errorf("error marshaling service data: %v", err)
	}

	err = writer.WriteMessages(context.Background(), kafka.Message{
		Key:   []byte(service.Name),
		Value: jsonData,
	})

	if err != nil {
		return fmt.Errorf("error sending registration message: %v", err)
	}

	log.Println("Service registration message sent successfully")
	return nil
}

// 서비스 등록 kafka producer 생성
func createRegistrationWriter() *kafka.Writer {
	return kafka.NewWriter(
		kafka.WriterConfig{
			Brokers:     []string{kafkaBroker},
			Topic:       registrationTopic,
			MaxAttempts: 5,
		})
}

// / 보조 지표 계산
func calculateIndicators(candles []lib.CandleData) (lib.TechnicalIndicators, error) {
	if len(candles) < 300 {
		return lib.TechnicalIndicators{}, fmt.Errorf("insufficient data: need at least 300 candles, got %d", len(candles))
	}

	prices := make([]float64, len(candles))
	highs := make([]float64, len(candles))
	lows := make([]float64, len(candles))

	for i, candle := range candles {
		price, err := strconv.ParseFloat(candle.Close, 64)
		if err != nil {
			return lib.TechnicalIndicators{}, fmt.Errorf("error parsing close price: %v", err)
		}
		prices[i] = price

		high, err := strconv.ParseFloat(candle.High, 64)
		if err != nil {
			return lib.TechnicalIndicators{}, fmt.Errorf("error parsing high price: %v", err)
		}
		highs[i] = high

		low, err := strconv.ParseFloat(candle.Low, 64)
		if err != nil {
			return lib.TechnicalIndicators{}, fmt.Errorf("error parsing low price: %v", err)
		}
		lows[i] = low
	}

	ema200 := calculate.CalculateEMA(prices, 200)
	macdLine, signalLine := calculate.CalculateMACD(prices)
	parabolicSAR := calculate.CalculateParabolicSAR(highs, lows)

	return lib.TechnicalIndicators{
		EMA200:       ema200,
		ParabolicSAR: parabolicSAR,
		MACDLine:     macdLine,
		SignalLine:   signalLine,
	}, nil
}

// signal 생성 함수
func generateSignal(candles []lib.CandleData, indicators lib.TechnicalIndicators) (string, lib.SignalConditions, float64, float64) {
	lastPrice, _ := strconv.ParseFloat(candles[len(candles)-1].Close, 64)
	lastHigh, _ := strconv.ParseFloat(candles[len(candles)-1].High, 64)
	lastLow, _ := strconv.ParseFloat(candles[len(candles)-1].Low, 64)

	conditions := lib.SignalConditions{
		Long: lib.SignalDetail{
			EMA200Condition:       lastPrice > indicators.EMA200,
			ParabolicSARCondition: indicators.ParabolicSAR < lastLow,
			MACDCondition:         indicators.MACDLine > indicators.SignalLine,
			EMA200Value:           indicators.EMA200,
			EMA200Diff:            lastPrice - indicators.EMA200,
			ParabolicSARValue:     indicators.ParabolicSAR,
			ParabolicSARDiff:      lastLow - indicators.ParabolicSAR,

			MACDHistogram: indicators.MACDLine - indicators.SignalLine,
		},
		Short: lib.SignalDetail{
			EMA200Condition:       lastPrice < indicators.EMA200,
			ParabolicSARCondition: indicators.ParabolicSAR > lastHigh,
			MACDCondition:         indicators.MACDLine < indicators.SignalLine,
			EMA200Value:           indicators.EMA200,
			EMA200Diff:            lastPrice - indicators.EMA200,
			ParabolicSARValue:     indicators.ParabolicSAR,
			ParabolicSARDiff:      indicators.ParabolicSAR - lastHigh,

			MACDHistogram: indicators.MACDLine - indicators.SignalLine,
		},
	}

	var stopLoss, takeProfit float64

	if conditions.Long.EMA200Condition && conditions.Long.ParabolicSARCondition && conditions.Long.MACDCondition {
		stopLoss = indicators.ParabolicSAR
		takeProfit = lastPrice + (lastPrice - stopLoss)
		return signalType.Long.String(), conditions, stopLoss, takeProfit
	} else if conditions.Short.EMA200Condition && conditions.Short.ParabolicSARCondition && conditions.Short.MACDCondition {
		stopLoss = indicators.ParabolicSAR
		takeProfit = lastPrice - (stopLoss - lastPrice)
		return signalType.Short.String(), conditions, stopLoss, takeProfit
	}
	return signalType.No_Signal.String(), conditions, 0.0, 0.0
}

func main() {
	log.Println("Starting Signal Service...")

	// 가격 read consumer
	reader := createReader()
	defer reader.Close()

	// signal write producer
	writer := createWriter()
	defer writer.Close()

	// service register producer
	registrationWriter := createRegistrationWriter()
	defer registrationWriter.Close()

	// register service
	if err := registerService(registrationWriter); err != nil {
		log.Printf("Failed to register service: %v\n", err)
	}

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

			var candles []lib.CandleData
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

				signalResult := lib.SignalResult{
					Signal:     signalType,
					Timestamp:  lastCandle.CloseTime,
					Price:      price,
					Conditions: conditions,
					StopLoss:   stopLoss,
					TakeProfie: takeProfit,
				}

				err = writeToKafka(writer, signalResult)

				if err != nil {
					log.Printf("Error sending signal to autro-notification: %v", err)
				} else {
					log.Printf("Signal sent to autro-notification. Response: %v", signalResult)
				}
			}
		}
	}
}
