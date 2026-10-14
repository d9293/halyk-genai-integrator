package httpx

import (
	"github.com/gin-gonic/gin"
)

const callerIDKey = "callerID"

// FixedCaller sets the same caller ID on every request. Auth is deferred in v1
// (decision 2026-10-13); a real auth middleware will replace this and set the
// caller from the verified principal.
func FixedCaller(callerID string) gin.HandlerFunc {
	return func(c *gin.Context) {
		c.Set(callerIDKey, callerID)
		c.Next()
	}
}

func CallerID(c *gin.Context) string {
	return c.GetString(callerIDKey)
}
