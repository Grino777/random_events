package models

import "github.com/nats-io/nats.go/jetstream"

type EventMessage interface {
	EventData
	EventChannels
}

type EventData interface {
	Accept() error
	Reject() error
	Data() []byte
}

type EventChannels interface {
	ResultChannel() chan error
	ConfirmChannel() chan error
	Close()
}

type BrokerMessage struct {
	natsMessage jetstream.Msg
	msg         []byte
	resultChan  chan error // Канал для чтения резульата обработки event в воркере
	confirmChan chan error // Канал для отправки подтверждения (Ack, Nak) сообщения в брокера
}

func NewBrokerMessage(msg jetstream.Msg) EventMessage {
	return &BrokerMessage{
		msg:         msg.Data(),
		resultChan:  make(chan error, 1),
		confirmChan: make(chan error, 1),
		natsMessage: msg,
	}
}

func (bm *BrokerMessage) Accept() error {
	if err := bm.natsMessage.Ack(); err != nil {
		return err
	}
	return nil
}

func (bm *BrokerMessage) Reject() error {
	if err := bm.natsMessage.Nak(); err != nil {
		return err
	}
	return nil
}

func (bm *BrokerMessage) Data() []byte {
	return bm.msg
}

func (bm *BrokerMessage) ResultChannel() chan error {
	return bm.resultChan
}

func (bm *BrokerMessage) ConfirmChannel() chan error {
	return bm.confirmChan
}

func (bm *BrokerMessage) Close() {
	close(bm.confirmChan)
	close(bm.resultChan)
}
