package publisher

import "errors"

// FailClosed synchronously removes one group's cached authority. Durable
// records remain in the outbox, but Accept and both dispatch paths stop using
// the group until a newer authoritative snapshot is applied.
func (p *Publisher) FailClosed(group string) error {
	if p == nil || group == "" {
		return errors.New("publisher: fail-closed group is required")
	}
	p.mu.Lock()
	delete(p.membership, group)
	p.mu.Unlock()
	return nil
}
