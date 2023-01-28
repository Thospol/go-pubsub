package main

import (
	"context"

	cps "github.com/Thospol/go-pubsub/pubsub"

	"cloud.google.com/go/pubsub"
	"github.com/sirupsen/logrus"
)

var (
	ctx = context.Background()
)

func main() {
	err := cps.NewClient("./config/sa-pubsub.json")
	if err != nil {
		panic(err)
	}

	client := cps.GetClient()
	id := "health"
	topic := client.GetTopic(id)
	res := topic.Publish(ctx, &pubsub.Message{
		Data: []byte(`{
			"topic_id": "phonebill",
            "event": "updated",
			"payload": {
			  "ref_id": 1,
			  "document_id": 1,
			  "approve_status": {
				"name": "ไม่อนุมัติ",
				"value": 3
			  }
			}
		  }`),
	})

	_, err = res.Get(ctx)
	if err != nil {
		logrus.Fatal(err)
	}
}
