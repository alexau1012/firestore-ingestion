package googleservices

import (
	"context"
	"log"

	"cloud.google.com/go/firestore"
	firebase "firebase.google.com/go"
)

func InitFirestore() (*firestore.Client, context.Context) {
	ctx := context.Background()
	conf := &firebase.Config{ProjectID: "pcone-xl-fb-dev"}

	app, err := firebase.NewApp(ctx, conf)
	if err != nil {
		log.Fatalln(err)
	}

	client, err := app.Firestore(ctx)
	if err != nil {
		log.Fatalln(err)
	}

	return client, ctx
}
