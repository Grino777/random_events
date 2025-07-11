package models

import "github.com/nats-io/nats.go/jetstream"

type BrokerMessage interface {
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

type brokerMessage struct {
	natsMessage jetstream.Msg
	msg         []byte
	resultChan  chan error // Канал для чтения резульата обработки event в воркере
	confirmChan chan error // Канал для отправки подтверждения (Ack, Nak) сообщения в брокера
}

func NewBrokerMessage(msg jetstream.Msg) BrokerMessage {
	return &brokerMessage{
		msg:         msg.Data(),
		resultChan:  make(chan error, 1),
		confirmChan: make(chan error, 1),
		natsMessage: msg,
	}
}

func (bm *brokerMessage) Accept() error {
	if err := bm.natsMessage.Ack(); err != nil {
		return err
	}
	return nil
}

func (bm *brokerMessage) Reject() error {
	if err := bm.natsMessage.Nak(); err != nil {
		return err
	}
	return nil
}

func (bm *brokerMessage) Data() []byte {
	return bm.msg
}

func (bm *brokerMessage) ResultChannel() chan error {
	return bm.resultChan
}

func (bm *brokerMessage) ConfirmChannel() chan error {
	return bm.confirmChan
}

func (bm *brokerMessage) Close() {
	close(bm.confirmChan)
	close(bm.resultChan)
}
