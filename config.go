package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"gopkg.in/yaml.v2"
)

type AccountConfig struct {
	TokenFile string `yaml:"token_file"`
	CloudHost string `yaml:"cloud_host"`
}

type ShareConfig struct {
	Name string `yaml:"name"`
	// keyed by account name → path
	Paths map[string]string `yaml:"paths"`
	// resolved document IDs (populated at runtime, persisted to state file)
	DocIDs map[string]string `yaml:"doc_ids,omitempty"`
}

type Config struct {
	Accounts map[string]AccountConfig `yaml:"accounts"`
	Shares   []ShareConfig            `yaml:"shares"`
	PollSecs int                      `yaml:"poll_secs"`
}

func LoadConfig(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("reading config: %w", err)
	}
	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("parsing config: %w", err)
	}
	if len(cfg.Accounts) < 2 {
		return nil, fmt.Errorf("config must define at least 2 accounts")
	}
	if len(cfg.Shares) == 0 {
		return nil, fmt.Errorf("config must define at least 1 share")
	}
	for name, acct := range cfg.Accounts {
		acct.TokenFile = expandPath(acct.TokenFile)
		cfg.Accounts[name] = acct
	}
	if cfg.PollSecs <= 0 {
		cfg.PollSecs = 30
	}
	for i := range cfg.Shares {
		if cfg.Shares[i].DocIDs == nil {
			cfg.Shares[i].DocIDs = make(map[string]string)
		}
		for acctName := range cfg.Shares[i].Paths {
			if _, ok := cfg.Accounts[acctName]; !ok {
				return nil, fmt.Errorf("share %q references unknown account %q", cfg.Shares[i].Name, acctName)
			}
		}
	}
	return &cfg, nil
}

func expandPath(p string) string {
	if strings.HasPrefix(p, "~/") {
		home, err := os.UserHomeDir()
		if err == nil {
			return filepath.Join(home, p[2:])
		}
	}
	return p
}
