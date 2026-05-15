package event

import (
	"time"
)

func (s *Service) RateLimiter() {
	ticker := time.NewTicker(time.Second / RATE)
	defer ticker.Stop()

	for {
		select {
		case <-s.ctx.Done():
			s.logger.Info("limiter stopped")
			return

		case <-ticker.C:
			select {
			case s.tokenChLimiter <- struct{}{}:
			default:
			}
		}
	}
}
