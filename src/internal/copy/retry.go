package copy

import "time"

// retrySchedule owns delay and diagnostic frequency, never partition completion.
type retrySchedule struct {
	next, logged time.Time
	delay        time.Duration
	attempts     int
}

func (s *retrySchedule) due(now time.Time) bool { return !now.Before(s.next) }
func (s *retrySchedule) failed(now time.Time) bool {
	s.attempts++
	s.delay = min(30*time.Second, max(time.Second, 2*s.delay))
	s.next = now.Add(s.delay)
	log := s.logged.IsZero() || now.Sub(s.logged) >= time.Minute
	if log {
		s.logged = now
	}
	return log
}
func (s *retrySchedule) recovered() int {
	attempts := s.attempts
	*s = retrySchedule{}
	return attempts
}
