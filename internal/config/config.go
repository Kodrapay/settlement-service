package config

import "os"

type Config struct {
	ServiceName        string
	Port               string
	MerchantServiceURL string
}

func Load(serviceName, defaultPort string) Config {
	return Config{
		ServiceName:        serviceName,
		Port:               getEnv("PORT", defaultPort),
		MerchantServiceURL: getEnv("MERCHANT_SERVICE_URL", "http://merchant-service:7002"),
	}
}

func getEnv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
