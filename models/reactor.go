package models

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"sync"
	"time"
	"reactor/utils"
)

type Reactor struct {
	log         *utils.Logger
	config      *Config
	buffer      *CircularBuffer
	subscribers map[net.Conn]chan bool
	subsMu      sync.RWMutex
	ctx         context.Context
	cancel      context.CancelFunc
	wg          sync.WaitGroup
	listener    net.Listener
	conn        net.Conn // соединение с predictor
	
	// Состояние alert
	isAlertActive   bool
	lastAlertTime   time.Time
	alertCooldown   time.Duration
	threshold       float64
	mu              sync.Mutex
}

func NewReactor(cfg *Config) *Reactor {
	ctx, cancel := context.WithCancel(context.Background())
	return &Reactor{
		log:           utils.GlobalLogger(),
		config:        cfg,
		buffer:        NewCircularBuffer(cfg.BufferSize),
		subscribers:   make(map[net.Conn]chan bool),
		ctx:           ctx,
		cancel:        cancel,
		threshold:     cfg.Alert.TemperatureThreshold,
		alertCooldown: time.Duration(cfg.Alert.AlertCooldownSeconds) * time.Second,
		isAlertActive: false,
	}
}

// Подключение к predictor
func (r *Reactor) connectToPredictor() error {
	conn, err := net.Dial("unix", r.config.UDSPredictorPath)
	if err != nil {
		return fmt.Errorf("failed to connect to predictor: %v", err)
	}
	r.conn = conn
	return nil
}

// Подписка на данные от predictor
func (r *Reactor) subscribeToPredictor() error {
	_, err := r.conn.Write([]byte("SUBSCRIBE\n"))
	if err != nil {
		return fmt.Errorf("failed to subscribe: %v", err)
	}
	r.log.Info("Subscribed to predictor")
	return nil
}

// Проверка температуры и создание алерта
func (r *Reactor) checkTemperature(prediction PredictionResult) *AlertMessage {
	r.mu.Lock()
	defer r.mu.Unlock()

	var alert *AlertMessage
	now := time.Now()

	if prediction.Prediction > r.threshold {
		// Проверяем cooldown
		if !r.isAlertActive || now.Sub(r.lastAlertTime) > r.alertCooldown {
			alert = &AlertMessage{
				Timestamp:   prediction.Timestamp,
				Temperature: prediction.Prediction,
				Threshold:   r.threshold,
				Status:      "CRITICAL",
				Message:     fmt.Sprintf("Temperature %.2f°C exceeds threshold %.2f°C!", 
					prediction.Prediction, r.threshold),
			}
			r.isAlertActive = true
			r.lastAlertTime = now
		}
	} else {
		// Если температура в норме, сбрасываем состояние алерта
		if r.isAlertActive && now.Sub(r.lastAlertTime) > r.alertCooldown {
			r.isAlertActive = false
			alert = &AlertMessage{
				Timestamp:   prediction.Timestamp,
				Temperature: prediction.Prediction,
				Threshold:   r.threshold,
				Status:      "OK",
				Message:     fmt.Sprintf("Temperature returned to normal: %.2f°C", prediction.Prediction),
			}
			r.log.Info("✅ Temperature normalized: %.2f°C", prediction.Prediction)
		}
	}

	return alert
}

// Получение данных от predictor
func (r *Reactor) receiveData() {
	defer r.wg.Done()
	
	decoder := json.NewDecoder(r.conn)
	for {
		select {
		case <-r.ctx.Done():
			return
		default:
			var prediction PredictionResult
			if err := decoder.Decode(&prediction); err != nil {
				r.log.Error("Error receiving data from predictor: %v", err)
				return
			}

			// Проверяем температуру
			alert := r.checkTemperature(prediction)
			
			if alert != nil {
				// Сохраняем в буфер
				r.buffer.Push(*alert)
				
				// Отправляем подписчикам
				r.notifySubscribers(*alert)
			}
		}
	}
}

// Обработка команд от клиентов
func (r *Reactor) handleConnection(conn net.Conn) {
	defer r.wg.Done()
	defer conn.Close()

	for {
		buf := make([]byte, 1024)
		n, err := conn.Read(buf)
		if err != nil {
			r.removeSubscriber(conn)
			return
		}

		command := string(buf[:n])
		switch command {
		case "GET\n", "GET\r\n":
			// Запрос всех алертов
			allData := r.buffer.GetAll()
			response, _ := json.Marshal(allData)
			conn.Write(response)

		case "GET_LAST\n", "GET_LAST\r\n":
			// Запрос последних 10 алертов
			lastData := r.buffer.GetLastN(10)
			response, _ := json.Marshal(lastData)
			conn.Write(response)

		case "STATUS\n", "STATUS\r\n":
			// Статус реактора
			r.mu.Lock()
			status := map[string]interface{}{
				"alert_active": r.isAlertActive,
				"threshold":    r.threshold,
				"last_alert":   r.lastAlertTime.Format(time.RFC3339),
				"alerts_count": r.buffer.count,
			}
			r.mu.Unlock()
			response, _ := json.Marshal(status)
			conn.Write(response)

		case "SUBSCRIBE\n", "SUBSCRIBE\r\n":
			r.log.Info("New subscriber to reactor")
			r.addSubscriber(conn)
			<-r.waitForUnsubscribe(conn)
			r.removeSubscriber(conn)
			return

		default:
			conn.Write([]byte("Unknown command\n"))
		}
	}
}

// Отправка данных подписчикам
func (r *Reactor) notifySubscribers(alert AlertMessage) {
	r.subsMu.RLock()
	defer r.subsMu.RUnlock()

	if len(r.subscribers) > 0 {
		data, err := json.Marshal(alert)
		if err != nil {
			r.log.Error("Error marshaling alert: %v", err)
			return
		}
		data = append(data, '\n')

		for conn := range r.subscribers {
			_, err := conn.Write(data)
			if err != nil {
				r.log.Error("Error sending alert to subscriber: %v", err)
			}
		}
	}
}

func (r *Reactor) addSubscriber(conn net.Conn) {
	r.subsMu.Lock()
	defer r.subsMu.Unlock()
	r.subscribers[conn] = make(chan bool)
}

func (r *Reactor) removeSubscriber(conn net.Conn) {
	r.subsMu.Lock()
	defer r.subsMu.Unlock()
	delete(r.subscribers, conn)
}

func (r *Reactor) waitForUnsubscribe(conn net.Conn) chan bool {
	ch := make(chan bool)
	go func() {
		buf := make([]byte, 1)
		conn.Read(buf)
		close(ch)
	}()
	return ch
}

// Запуск реактора
func (r *Reactor) Start() error {
	// Подключаемся к predictor
	if err := r.connectToPredictor(); err != nil {
		return err
	}

	// Подписываемся на данные
	if err := r.subscribeToPredictor(); err != nil {
		return err
	}

	// Создаем UDS сервер
	os.Remove(r.config.UDSSocketPath)
	listener, err := net.Listen("unix", r.config.UDSSocketPath)
	if err != nil {
		return fmt.Errorf("failed to listen on UDS: %v", err)
	}
	r.listener = listener

	// Запускаем получение данных
	r.wg.Add(1)
	go r.receiveData()

	// Принимаем соединения от клиентов
	r.wg.Add(1)
	go func() {
		defer r.wg.Done()
		for {
			select {
			case <-r.ctx.Done():
				return
			default:
				conn, err := listener.Accept()
				if err != nil {
					select {
					case <-r.ctx.Done():
						return
					default:
						continue
					}
				}
				r.wg.Add(1)
				go r.handleConnection(conn)
			}
		}
	}()

	r.log.Info("Reactor started successfully")
	r.log.Info("Temperature threshold: %.2f°C", r.threshold)
	r.log.Info("Alert cooldown: %v", r.alertCooldown)
	return nil
}

// Остановка реактора
func (r *Reactor) Stop() error {
	r.cancel()

	if r.conn != nil {
		r.conn.Close()
	}

	if r.listener != nil {
		r.listener.Close()
	}

	done := make(chan struct{})
	go func() {
		r.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		return nil
	case <-time.After(30 * time.Second):
		return fmt.Errorf("timeout waiting for goroutines to stop")
	}
}