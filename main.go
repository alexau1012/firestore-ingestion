package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"time"

	"cloud.google.com/go/firestore"
	googleservices "github.com/alexau1012/firestore-data-ingestion/google-services/firestore"
	"google.golang.org/api/iterator"
)

type Meta struct {
	Type string `json:"type"`
	Ver  string `json:"ver"`
}

type Recommendations struct {
	recommendations []string
	meta            Meta
}

type Config struct {
	UserIds         []string `json:"userIds"`
	Recommendations []string `json:"recommendations"`
	Meta            Meta     `json:"meta"`
}

func main() {
	configFilePtr := flag.String("config", "", "JSON file config")
	flag.Parse()
	config := readConfigFile(*configFilePtr)

	client, ctx := googleservices.InitFirestore()

	for _, userId := range config.UserIds {
		cref := fmt.Sprintf("users/%v/personalisedShowRecommendations", userId)

		dref := fmt.Sprintf("personalisedShowRecommendations/%v/personalisedShows/recommendations", userId)

		fmt.Println("Before Ingestion")
		err := readCollection(ctx, client, cref)
		if err != nil {
			log.Fatalln(err)
		}

		err = deleteCollection(ctx, client, cref, 20)
		if err != nil {
			log.Fatalln(err)
		}

		// Set recommendation ids
		setDocument(ctx, client, dref, &Recommendations{meta: config.Meta, recommendations: config.Recommendations})

		time.Sleep(5 * time.Second)

		fmt.Println("After Ingestion")
		readCollection(ctx, client, cref)
	}

	defer client.Close()
}

func readConfigFile(configFileName string) Config {
	configFile, err := os.Open(configFileName)
	if err != nil {
		log.Fatalln(err)
	}
	defer configFile.Close()

	byteValue, _ := io.ReadAll(configFile)

	var config Config

	json.Unmarshal(byteValue, &config)

	return config
}

func readCollection(ctx context.Context, client *firestore.Client, collectionName string) error {
	docs := client.Collection(collectionName).Documents(ctx)
	count := 0
	for {
		_, err := docs.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return err
		}
		// dsnap := doc.Data()
		// dsnapPretty, err := json.MarshalIndent(dsnap, "", "  ")
		// if err != nil {
		// 	return err
		// }
		// fmt.Print(string(dsnapPretty) + "\n")
		count++
	}
	fmt.Printf("%v: Document count: %d\n", collectionName, count)
	return nil
}

func deleteCollection(ctx context.Context, client *firestore.Client, collectionName string, batchSize int) error {
	col := client.Collection(collectionName)
	bulkwriter := client.BulkWriter(ctx)

	for {
		// Get a batch of documents
		iter := col.Limit(batchSize).Documents(ctx)
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

func setDocument(ctx context.Context, client *firestore.Client, documentName string, value *Recommendations) error {
	fmt.Println(value)
	_, err := client.Doc(documentName).Set(ctx, map[string]interface{}{
		"recommendations": value.recommendations,
		"meta": map[string]interface{}{
			"type": value.meta.Type,
			"ver":  value.meta.Ver,
		},
	})
	fmt.Printf("Set document \"%s\"\n", documentName)
	return err
}
