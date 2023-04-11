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
	usecasePtr := flag.String("usecase", "READ_ONLY", "Specify use case: READ_ONLY / READ_WRITE / RESET")
	entityPtr := flag.String("entity", "shows", "shows / episodes")
	verbosePtr := flag.Bool("verbose", false, "Full logs")

	recommendationsKey := *entityPtr
	if *entityPtr == "shows" {
		recommendationsKey = "recommendations"
	}

	flag.Parse()
	config := readConfigFile(*configFilePtr)

	db := firestoreDB.New()

	for _, userId := range config.UserIds {
		recommendationIdsPath, userFeed := getRefs(*entityPtr, userId)

		var err error
		switch *usecasePtr {
		case "RESET":
			fmt.Printf("Resetting user <%v> recommendations feed...\n", userId)
			err = db.DeleteCollection(recommendationIdsPath)
			fmt.Println()
		case "READ_ONLY":
			fmt.Printf("Reading user <%v> recommendation ids...\n", userId)
			err = db.ReadCollection(recommendationIdsPath, *verbosePtr)
			fmt.Println()
		case "READ_WRITE":
			fmt.Printf("Ingesting user <%v> recommendation ids...\n", userId)
			err = db.ReadCollection(recommendationIdsPath, *verbosePtr)
			if err != nil {
				break
			}
			err = db.SetDocument(userFeed,
				&domain.Recommendations{Recommendations: config.Recommendations, Meta: config.Meta}, recommendationsKey)
			if err != nil {
				break
			}
			time.Sleep(3 * time.Second)
			err = db.ReadCollection(userFeed, *verbosePtr)
			fmt.Println()
		}
		if err != nil {
			log.Fatalln(err)
		}
	}

	defer db.CloseConn()
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

	return fmt.Sprintf(userFeed, userId), fmt.Sprintf(recommendationIdsPath, userId)
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
