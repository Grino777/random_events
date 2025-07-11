package models

import (
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"math/rand"
)

const (
	opEvent = "domain.event."
)

const (
	payloadLenght = 10
	idLenght      = 4
	maxSenderId   = 100
	timeDelta     = 10
)

type Event struct {
	SenderId  string
	CreatedAt int64
	Payload   string
	ExecTime  float64
}

func NewEvent() (*Event, error) {
	createdAt := time.Now().Unix()
	event := &Event{CreatedAt: createdAt}

	if err := event.generateRandomPayload(); err != nil {
		return nil, err
	}
	event.generateRandomId()
	event.generateRandomExecTime()

	return event, nil
}

func (e *Event) generateRandomPayload() error {
	const letters = "abcdefghijklmnopqrstuvwxyz"
	bytes := make([]byte, payloadLenght)
	for i := range bytes {
		bytes[i] = letters[rand.Intn(len(letters))]
	}
	e.Payload = string(bytes)
	return nil
}

func (e *Event) generateRandomId() {
	randSenderId := rand.Intn(maxSenderId)
	strId := strconv.Itoa(randSenderId)
	e.SenderId = strId
}

func (e *Event) generateRandomExecTime() {
	randFloat := rand.Float64()
	randTime := randFloat * 10.0
	e.ExecTime = randTime
}

func (e *Event) ToJSON() ([]byte, error) {
	const op = opEvent + "ToJSON"
	data, err := json.Marshal(e)
	if err != nil {
		return nil, fmt.Errorf("%s: %v", op, err)
	}
	return data, nil
}

func EventFromJSON(data []byte) (*Event, error) {
	const op = opEvent + "FromJSON"

	var event *Event
	err := json.Unmarshal(data, &event)
	if err != nil {
		return nil, err
	}
	return event, nil
}
