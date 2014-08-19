package nats

import (
	"github.com/apcera/nats"
)

type mockConnection struct {
	msgs chan *nats.Msg
}

func (conn *mockConnection) Publish(subject string, data []byte) error {

	return nil
}

func (conn *mockConnection) Subscribe(subject string, cb nats.MsgHandler) (*nats.Subscription, error) {
	go func() {
		var (
			msg *nats.Msg
			ok  bool
		)

		for {
			msg, ok = <-conn.msgs
			if !ok {
				return
			}
			cb(msg)
		}
	}()
	return nil, nil
}

func (conn *mockConnection) Close() {

}

type Connection interface {
	Publish(string, []byte) error
	Subscribe(string, nats.MsgHandler) (*nats.Subscription, error)
	Close()
}
