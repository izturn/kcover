package config

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

func Load(path string) (Agent, error) {
	if path == "" {
		cfg := DefaultAgent()
		cfg.ApplyDefaults()
		return cfg, nil
	}

	data, err := os.ReadFile(path)
	if err != nil {
		return Agent{}, fmt.Errorf("read config file %q: %w", path, err)
	}

	cfg := DefaultAgent()
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return Agent{}, fmt.Errorf("unmarshal config file %q: %w", path, err)
	}

	cfg.ApplyDefaults()
	return cfg, nil
}
