package signal

type Signal chan struct{}

func (s Signal) Send() {
	select {
	case s <- struct{}{}:
	default:
	}
}
