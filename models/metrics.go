package models

import (
	"sync"
)

// PredictionResult - результат от predictor
type PredictionResult struct {
	Timestamp  int64   `json:"timestamp"`
	Prediction float64 `json:"prediction"`
	Actual     float64 `json:"actual,omitempty"`
}

// AlertMessage - сообщение об ошибке
type AlertMessage struct {
	Timestamp   int64   `json:"timestamp"`
	Temperature float64 `json:"temperature"`
	Threshold   float64 `json:"threshold"`
	Status      string  `json:"status"` // "CRITICAL", "WARNING", "OK"
	Message     string  `json:"message"`
}

// CircularBuffer для хранения истории
type CircularBuffer struct {
	data  []AlertMessage
	size  int
	head  int
	count int
	mu    sync.RWMutex
}

func NewCircularBuffer(size int) *CircularBuffer {
	return &CircularBuffer{
		data:  make([]AlertMessage, size),
		size:  size,
		head:  0,
		count: 0,
	}
}

func (cb *CircularBuffer) Push(msg AlertMessage) {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	cb.data[cb.head] = msg
	cb.head = (cb.head + 1) % cb.size
	if cb.count < cb.size {
		cb.count++
	}
}

func (cb *CircularBuffer) GetAll() []AlertMessage {
	cb.mu.RLock()
	defer cb.mu.RUnlock()

	result := make([]AlertMessage, cb.count)
	for i := 0; i < cb.count; i++ {
		idx := (cb.head - cb.count + i) % cb.size
		if idx < 0 {
			idx += cb.size
		}
		result[i] = cb.data[idx]
	}
	return result
}

// GetLastN возвращает последние N сообщений
func (cb *CircularBuffer) GetLastN(n int) []AlertMessage {
	cb.mu.RLock()
	defer cb.mu.RUnlock()

	if n > cb.count {
		n = cb.count
	}

	result := make([]AlertMessage, n)
	for i := 0; i < n; i++ {
		idx := (cb.head - n + i) % cb.size
		if idx < 0 {
			idx += cb.size
		}
		result[i] = cb.data[idx]
	}
	return result
}