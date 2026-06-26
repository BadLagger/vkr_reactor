package models

import (
	"encoding/json"
	"fmt"
	"os"
)

type AlertConfig struct {
	TemperatureThreshold float64 `json:"temperature_threshold"`
	CheckIntervalSeconds int     `json:"check_interval_seconds"`
	AlertCooldownSeconds int     `json:"alert_cooldown_seconds"`
}

type Config struct {
	UDSPredictorPath    string       `json:"uds_predictor_path"`
	UDSSocketPath       string       `json:"uds_socket_path"`
	PollIntervalSeconds int          `json:"poll_interval_seconds"`
	BufferSize          int          `json:"buffer_size"`
	Alert               AlertConfig  `json:"alert"`
	LogFile             string       `json:"log_file,omitempty"`
}

func NewConfig(configPath string) (*Config, error) {
	data, err := os.ReadFile(configPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read config: %v", err)
	}

	var config Config
	if err := json.Unmarshal(data, &config); err != nil {
		return nil, fmt.Errorf("failed to parse config: %v", err)
	}
	return &config, nil
}