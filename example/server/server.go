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
	id := "pss-approves"
	topic := client.GetTopic(id)
	res := topic.Publish(ctx, &pubsub.Message{
		Data: []byte(`{
			"topic_id": "phonebill",
            "event": "updated",
			"payload": {
			  "ref_id": 1,
			  "document_id": 1,
			  "approve_status": {
				"name": "รออนุมัติ",
				"value": 1
			  }
			}
		  }`),
	})

	_, err = res.Get(ctx)
	if err != nil {
		logrus.Fatal(err)
	}
}
