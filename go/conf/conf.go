package conf

import (
	"encoding/json"
	"io"
	"os"
)

type Config struct {
	Port int `json:"port"`
}

func LoadConfig(path string) (*Config, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	str, err := io.ReadAll(file)
	if err != nil {
		return nil, err
	}

	config := &Config{}
	if err = json.Unmarshal(str, config); err != nil {
		return nil, err
	}

	return config, nil
}
