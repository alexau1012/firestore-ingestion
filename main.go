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
	firestoreDB "github.com/alexau1012/firestore-data-ingestion/firestore_db"
)

func main() {
	configFilePtr := flag.String("config", "", "JSON file config")
	usecasePtr := flag.String("usecase", "WRITE", "Specify use case: READ / WRITE / RESET")
	entityPtr := flag.String("entity", "shows", "shows / episodes")
	verbosePtr := flag.Bool("verbose", false, "Full logs")

	flag.Parse()

	recommendationsKey := *entityPtr
	if *entityPtr == "shows" {
		recommendationsKey = "recommendations"
	}

	config := readConfigFile(*configFilePtr)

	db := firestoreDB.New()

	for _, userId := range config.UserIds {
		recommendationIdsPath, userFeed := getRefs(*entityPtr, userId)

		var err error
		switch *usecasePtr {
		case "RESET":
			fmt.Printf("Resetting user <%v> recommendations feed...\n", userId)
			err = db.DeleteCollection(userFeed)
			fmt.Println()
		case "READ":
			fmt.Printf("Reading user <%v> recommended feed...\n", userId)
			err = db.ReadCollection(userFeed, *verbosePtr)
			fmt.Println()
		case "WRITE":
			fmt.Printf("Ingesting user <%v> recommendation ids...\n", userId)
			err = db.ReadCollection(userFeed, *verbosePtr)
			if err != nil {
				break
			}
			err = db.SetDocument(recommendationIdsPath,
				&domain.Recommendations{Recommendations: config.Recommendations, Meta: config.Meta}, recommendationsKey)
			if err != nil {
				break
			}
			time.Sleep(5 * time.Second)
			err = db.ReadCollection(userFeed, *verbosePtr)
			fmt.Println()
		}
		if err != nil {
			log.Fatalln(err)
		}
	}

	defer db.CloseConnection()
}

func getRefs(entity string, userId string) (cref string, dref string) {
	var recommendationIdsPath string
	var userFeed string

	switch entity {
	case "episodes":
		recommendationIdsPath = "recommendations/%v/trendingEpisodes/episodes"
		userFeed = "users/%v/feed/sections/bottom"
	case "shows":
		recommendationIdsPath = "personalisedShowRecommendations/%v/personalisedShows/recommendations"
		userFeed = "users/%v/personalisedShowRecommendations"
	}

	return fmt.Sprintf(recommendationIdsPath, userId), fmt.Sprintf(userFeed, userId)
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
