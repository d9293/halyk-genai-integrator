package httpx

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
)

func testContext(ctx context.Context) (*gin.Context, *httptest.ResponseRecorder) {
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequestWithContext(ctx, http.MethodPost, "/internal/v1/x", nil)
	return c, w
}

func TestClientGone(t *testing.T) {
	gone, cancel := context.WithCancel(context.Background())
	cancel()
	canceled := fmt.Errorf("renew claim: %w", context.Canceled)

	tests := []struct {
		name string
		ctx  context.Context
		err  error
		want bool
	}{
		{"caller hung up", gone, canceled, true},
		{"cancelled while the caller still waits is a real error", context.Background(), canceled, false},
		{"other error after the caller hung up", gone, errors.New("db down"), false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c, _ := testContext(tt.ctx)
			if got := ClientGone(c, tt.err); got != tt.want {
				t.Errorf("ClientGone = %v, want %v", got, tt.want)
			}
			if c.IsAborted() != tt.want {
				t.Errorf("aborted = %v, want %v", c.IsAborted(), tt.want)
			}
		})
	}
}
