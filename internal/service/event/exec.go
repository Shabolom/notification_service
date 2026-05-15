package event

import "go.uber.org/zap"

func (s *Service) Exec() {
	err, loginConsumer, registerConsumer := s.rabbit.GettingConsumers()
	if err != nil {
		s.logger.Error("Getting events failed", zap.Error(err))
		return
	}

	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		s.RateLimiter()
	}()

	s.loginWorkersStart(loginConsumer)
	s.registerWorkersStart(registerConsumer)
}
