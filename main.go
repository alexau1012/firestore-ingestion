package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"time"

	"github.com/alexau1012/firestore-data-ingestion/domain"
	googleservices "github.com/alexau1012/firestore-data-ingestion/google-services/firestore"
)

func main() {
	configFilePtr := flag.String("config", "", "JSON file config")
	flag.Parse()
	config := readConfigFile(*configFilePtr)

	client, ctx := googleservices.InitFirestore()

	for _, userId := range config.UserIds {
		cref := fmt.Sprintf("users/%v/personalisedShowRecommendations", userId)

		dref := fmt.Sprintf("personalisedShowRecommendations/%v/personalisedShows/recommendations", userId)

		fmt.Println("Before Ingestion")
		err := googleservices.ReadCollection(ctx, client, cref, false)
		if err != nil {
			log.Fatalln(err)
		}

		err = googleservices.DeleteCollection(ctx, client, cref, 20)
		if err != nil {
			log.Fatalln(err)
		}

		// Set recommendation ids
		googleservices.SetDocument(ctx, client, dref, &domain.Recommendations{Meta: config.Meta, Recommendations: config.Recommendations})

		time.Sleep(5 * time.Second)

		fmt.Println("After Ingestion")
		googleservices.ReadCollection(ctx, client, cref, false)
	}

	defer client.Close()
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
