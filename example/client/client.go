package main

import (
	"context"
	"sync"

	ps "github.com/Thospol/go-pubsub/pubsub"

	"cloud.google.com/go/pubsub"
	jsoniter "github.com/json-iterator/go"
	"github.com/sirupsen/logrus"
)

func main() {
	err := ps.NewClient("./config/sa-pubsub.json")
	if err != nil {
		panic(err)
	}

	srv := NewService()
	err = srv.Subscribe()
	if err != nil {
		panic(err)
	}
}

// Service service interface
type Service interface {
	Subscribe() error
}

type service struct {
	mu           sync.Mutex
	pubsubClient ps.Client
}

func NewService() Service {
	return &service{
		pubsubClient: ps.GetClient(),
	}
}

// Data data
type Data struct {
	TopicID string `json:"topic_id"`
	Event   string `json:"event"`
	Payload struct {
		RefID         int `json:"ref_id"`
		DocumentID    int `json:"document_id"`
		ApproveStatus struct {
			Name  string `json:"name"`
			Value int    `json:"value"`
		} `json:"approve_status"`
	} `json:"payload"`
}

// Subscribe subscribe
func (s *service) Subscribe() error {
	var (
		id = "health-sub"
	)
	onMessage := func(ctx context.Context, msg *pubsub.Message) {
		s.mu.Lock()
		defer s.mu.Unlock()
		var json = jsoniter.ConfigCompatibleWithStandardLibrary
		data := &Data{}
		err := json.Unmarshal(msg.Data, data)
		if err != nil {
			logrus.Errorf("unmarshal error: %v", err)
			return
		}

		switch data.TopicID {
		case "phonebill":
			logrus.Infof("data: %+v", data)
		default:
			// TODO: ...
		}
		// msg.Nack() // call this method when process error pub/sub will to retry immediately for send message again
		msg.Ack() // call this method when process success
	}

	err := s.pubsubClient.Subscribe(id, onMessage)
	if err != nil {
		return err
	}

	return err
}
