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
	"github.com/alexau1012/firestore-data-ingestion/domain"
	googleservices "github.com/alexau1012/firestore-data-ingestion/google-services/firestore"
)

func main() {
	configFilePtr := flag.String("config", "", "JSON file config")
	usecasePtr := flag.String("usecase", "READ_ONLY", "Specify use case: READ_ONLY / READ_WRITE / RESET")
	flag.Parse()
	config := readConfigFile(*configFilePtr)

	client, ctx := googleservices.InitFirestore()

	for _, userId := range config.UserIds {
		cref := fmt.Sprintf("users/%v/personalisedShowRecommendations", userId)
		dref := fmt.Sprintf("personalisedShowRecommendations/%v/personalisedShows/recommendations", userId)

		var err error
		if *usecasePtr == "RESET" {
			err = resetUserRecommmendations(ctx, client, cref, userId)
			if err != nil {
				log.Fatalln(err)
			}
		}

		if *usecasePtr == "READ_ONLY" {
			err = readUserRecommendations(ctx, client, cref, false, userId)
			if err != nil {
				log.Fatalln(err)
			}
		}

		if *usecasePtr == "READ_WRITE" {
			fmt.Printf("Ingesting user <%v> recommendation ids...", userId)
			err = readUserRecommendations(ctx, client, cref, false, userId)
			if err != nil {
				log.Fatalln(err)
			}
			err = writeUserRecommendationIds(ctx, client, dref,
				&domain.Recommendations{Recommendations: config.Recommendations, Meta: config.Meta})
			if err != nil {
				log.Fatalln(err)
			}
			err = readUserRecommendations(ctx, client, cref, false, userId)
			if err != nil {
				log.Fatalln(err)
			}
		}

		// Set recommendation ids
		googleservices.SetDocument(ctx, client, dref, &domain.Recommendations{Meta: config.Meta, Recommendations: config.Recommendations})

		time.Sleep(5 * time.Second)

		fmt.Println("After Ingestion")
		googleservices.ReadCollection(ctx, client, cref, false)
	}

	defer client.Close()
}

func resetUserRecommmendations(ctx context.Context, client *firestore.Client, cref string, userId string) error {
	fmt.Printf("Resetting user <%v> recommendations collection...", userId)
	err := googleservices.DeleteCollection(ctx, client, cref, 20)
	return err
}

func readUserRecommendations(ctx context.Context, client *firestore.Client, cref string, printDocuments bool, userId string) error {
	fmt.Printf("Reading user <%v> recommendations collection...", userId)
	err := googleservices.ReadCollection(ctx, client, cref, false)
	return err
}

func writeUserRecommendationIds(ctx context.Context, client *firestore.Client, documentName string, value *domain.Recommendations) error {
	err := googleservices.SetDocument(ctx, client, documentName, value)
	return err
}

func readConfigFile(configFileName string) domain.Config {
	configFile, err := os.Open(configFileName)
	if err != nil {
		log.Fatalln(err)
	}
	defer configFile.Close()

	byteValue, _ := io.ReadAll(configFile)

	var config domain.Config

	json.Unmarshal(byteValue, &config)

	return config
}
