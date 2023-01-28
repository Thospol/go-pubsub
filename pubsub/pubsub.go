package pubsub

import (
	"context"
	"os"
	"sync"

	"cloud.google.com/go/pubsub"
	"github.com/sirupsen/logrus"
	"github.com/tidwall/gjson"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
)

var (
	ctx       = context.Background()
	mu        = sync.Mutex{}
	C         = &pubsub.Client{}
	OnMessage = func(ctx context.Context, msg *pubsub.Message) {
		mu.Lock()
		logrus.Infof("receive message: %s", string(msg.Data))
		defer mu.Unlock()
		msg.Ack()
	}
)

// Client google cloud pub/sub client interface
type Client interface {
	Publish(topicID, message string) error
	Subscribe(subscriptionID string, f func(context.Context, *pubsub.Message)) error
	NewTopic(topicID string) (*pubsub.Topic, error)
	GetTopics() ([]*pubsub.Topic, error)
	GetTopic(id string) *pubsub.Topic
	NewSubscription(subscriptionID string, topic *pubsub.Topic) error
	GetSubscriptions() ([]*pubsub.Subscription, error)
}

type client struct {
	*pubsub.Client
}

// NewClient google cloud pub/sub create a new client
func NewClient(filename string) error {
	opt := option.WithCredentialsFile(filename)
	content, err := os.ReadFile(filename)
	if err != nil {
		return err
	}

	value := gjson.Get(string(content), "project_id")
	C, err = pubsub.NewClient(ctx, value.String(), opt)
	if err != nil {
		return err
	}

	return nil
}

// GetClient  google cloud pub/sub get client
func GetClient() Client {
	return &client{C}
}

// Publish publish message to topic
func (c *client) Publish(topicID, message string) error {
	topic := c.Topic(topicID)
	res := topic.Publish(ctx, &pubsub.Message{
		Data: []byte(message),
	})

	_, err := res.Get(ctx)
	if err != nil {
		return err
	}

	return nil
}

// Subscribe subscription subscription by sub scription_id
func (c *client) Subscribe(subscriptionID string, f func(context.Context, *pubsub.Message)) error {
	sub := c.Subscription(subscriptionID)
	err := sub.Receive(ctx, f)
	if err != nil {
		return err
	}

	return nil
}

// NewTopic create topic
func (c *client) NewTopic(topicID string) (*pubsub.Topic, error) {
	topic, err := c.CreateTopic(ctx, topicID)
	if err != nil {
		return nil, err
	}

	return topic, nil
}

// NewSubscription new subscription
func (c *client) NewSubscription(subscriptionID string, topic *pubsub.Topic) error {
	config := pubsub.SubscriptionConfig{Topic: topic}
	_, err := c.CreateSubscription(ctx, subscriptionID, config)
	if err != nil {
		return err
	}

	return nil
}

// GetTopics list all topics
func (c *client) GetTopics() ([]*pubsub.Topic, error) {
	var topics []*pubsub.Topic
	it := c.Topics(ctx)
	for {
		topic, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, err
		}
		topics = append(topics, topic)
	}

	return topics, nil
}

// GetSubscriptions list all subscriptions
func (c *client) GetSubscriptions() ([]*pubsub.Subscription, error) {
	var subscriptions []*pubsub.Subscription
	it := c.Subscriptions(ctx)
	for {
		subscription, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, err
		}
		subscriptions = append(subscriptions, subscription)
	}

	return subscriptions, nil
}

// Topic topic
func (c *client) GetTopic(id string) *pubsub.Topic {
	return c.Topic(id)
}
