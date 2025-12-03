package middleware

import (
	"fmt"
	"time"

	"github.com/gofiber/fiber/v2"
)

func RequestID() fiber.Handler {
	return func(c *fiber.Ctx) error {
		requestID := c.Get("X-Request-ID")
		if requestID == "" {
			requestID = fmt.Sprintf("%d", time.Now().UnixNano())
		}
		c.Set("X-Request-ID", requestID)
		return c.Next()
	}
}
