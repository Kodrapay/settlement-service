package config

import "os"

type Config struct {
	ServiceName string
	Port        string
}

func Load(serviceName, defaultPort string) Config {
	return Config{
		ServiceName: serviceName,
		Port:        getEnv("PORT", defaultPort),
	}
}

func getEnv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
