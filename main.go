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
	flag.Parse()
	config := readConfigFile(*configFilePtr)

	db := firestoreDB.New()

	for _, userId := range config.UserIds {
		cref := fmt.Sprintf("users/%v/personalisedShowRecommendations", userId)
		dref := fmt.Sprintf("personalisedShowRecommendations/%v/personalisedShows/recommendations", userId)

		var err error
		switch *usecasePtr {
		case "RESET":
			fmt.Printf("Resetting user <%v> recommendations feed...", userId)
			err = db.DeleteCollection(cref)
		case "READ_ONLY":
			fmt.Printf("Reading user <%v> recommendation ids...", userId)
			err = db.ReadCollection(cref, false)
		case "READ_WRITE":
			fmt.Printf("Ingesting user <%v> recommendation ids...", userId)
			err = db.ReadCollection(cref, false)
			if err != nil {
				break
			}
			err = db.SetDocument(dref,
				&domain.Recommendations{Recommendations: config.Recommendations, Meta: config.Meta})
			if err != nil {
				break
			}
			time.Sleep(3 * time.Second)
			err = db.ReadCollection(cref, false)
		}
		if err != nil {
			log.Fatalln(err)
		}
	}

	defer db.CloseConn()
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
