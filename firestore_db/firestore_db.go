package firestore_db

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	"cloud.google.com/go/firestore"
	firebase "firebase.google.com/go"
	"github.com/alexau1012/firestore-data-ingestion/domain"
	"github.com/alexau1012/firestore-data-ingestion/infrastructure"
	"google.golang.org/api/iterator"
)

type FirestoreDB struct {
	client    *firestore.Client
	ctx       context.Context
	batchSize int
}

func New(projectId string) infrastructure.DocumentDatabase {
	ctx := context.Background()
	conf := &firebase.Config{ProjectID: projectId}

	app, err := firebase.NewApp(ctx, conf)
	if err != nil {
		log.Fatalln(err)
	}

	client, err := app.Firestore(ctx)
	if err != nil {
		log.Fatalln(err)
	}

	return FirestoreDB{client: client, ctx: ctx, batchSize: 20}
}

func (db FirestoreDB) CloseConn() {
	db.client.Close()
}

func (db FirestoreDB) ReadCollection(collectionName string, printDocuments bool) error {
	collection := db.client.Collection(collectionName)
	if collection == nil {
		fmt.Println("Collection does not exist")
		return nil
	}
	docs := collection.Documents(db.ctx)
	count := 0
	for {
		doc, err := docs.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return err
		}
		if printDocuments {
			dsnap := doc.Data()
			dsnapPretty, err := json.MarshalIndent(dsnap, "", "  ")
			if err != nil {
				return err
			}
			fmt.Println(string(dsnapPretty))
		}
		count++
	}
	fmt.Printf("%v: Document count: %d\n", collectionName, count)
	return nil
}

func (db FirestoreDB) DeleteCollection(collectionName string) error {
	col := db.client.Collection(collectionName)
	bulkwriter := db.client.BulkWriter(db.ctx)

	for {
		// Get a batch of documents
		iter := col.Limit(db.batchSize).Documents(db.ctx)
		numDeleted := 0

		// Iterate through the documents, adding
		// a delete operation for each one to the BulkWriter.
		for {
			doc, err := iter.Next()
			if err == iterator.Done {
				break
			}
			if err != nil {
				return err
			}

			bulkwriter.Delete(doc.Ref)
			numDeleted++
		}

		// If there are no documents to delete,
		// the process is over.
		if numDeleted == 0 {
			bulkwriter.End()
			break
		}

		bulkwriter.Flush()
	}
	fmt.Printf("Deleted collection \"%s\"", collectionName)
	return nil
}

func (db FirestoreDB) SetDocument(documentName string, value *domain.Recommendations, key string) error {
	_, err := db.client.Doc(documentName).Set(db.ctx, map[string]interface{}{
		key: value.Recommendations,
		"meta": map[string]interface{}{
			"type": value.Meta.Type,
			"ver":  value.Meta.Ver,
		},
	})
	fmt.Printf("Set document \"%s\"\n", documentName)
	return err
}

func (db FirestoreDB) CloseConnection() {
	db.CloseConn()
}
