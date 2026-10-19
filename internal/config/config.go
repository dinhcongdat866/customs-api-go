// Package config reads runtime settings from the environment.
// A .env file in the working directory is loaded first (never committed).
package config

import (
	"bufio"
	"fmt"
	"os"
	"strconv"
	"strings"
)

type Config struct {
	DatabaseURL string
	Port        string
	Workers     int
}

func Load() (Config, error) {
	loadDotEnv(".env")

	cfg := Config{
		DatabaseURL: os.Getenv("DATABASE_URL"),
		Port:        envOr("PORT", "8080"),
		Workers:     4,
	}
	if cfg.DatabaseURL == "" {
		return cfg, fmt.Errorf("DATABASE_URL is required")
	}
	if w := os.Getenv("WORKERS"); w != "" {
		n, err := strconv.Atoi(w)
		if err != nil || n < 1 {
			return cfg, fmt.Errorf("WORKERS must be a positive integer, got %q", w)
		}
		cfg.Workers = n
	}
	return cfg, nil
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// loadDotEnv sets KEY=VALUE lines from the file, without overriding
// variables that are already set in the real environment.
func loadDotEnv(path string) {
	f, err := os.Open(path)
	if err != nil {
		return // no .env is fine
	}
	defer f.Close()

	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		key, val, ok := strings.Cut(line, "=")
		if !ok {
			continue
		}
		key = strings.TrimSpace(key)
		val = strings.Trim(strings.TrimSpace(val), `"'`)
		if os.Getenv(key) == "" {
			os.Setenv(key, val)
		}
	}
}
