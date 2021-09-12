package signal

type Signal chan struct{}

func New() Signal {
	return make(Signal, 1)
}

func (s Signal) Send() {
	select {
	case s <- struct{}{}:
	default:
	}
}
