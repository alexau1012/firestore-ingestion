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
		if *usecasePtr == "RESET" {
			err = resetUserRecommmendations(db, cref, userId)
			if err != nil {
				log.Fatalln(err)
			}
		}

		if *usecasePtr == "READ_ONLY" {
			err = readUserRecommendations(db, cref, false, userId)
			if err != nil {
				log.Fatalln(err)
			}
		}

		if *usecasePtr == "READ_WRITE" {
			fmt.Printf("Ingesting user <%v> recommendation ids...", userId)
			err = readUserRecommendations(db, cref, false, userId)
			if err != nil {
				log.Fatalln(err)
			}
			err = writeUserRecommendationIds(db, dref,
				&domain.Recommendations{Recommendations: config.Recommendations, Meta: config.Meta})
			if err != nil {
				log.Fatalln(err)
			}

			time.Sleep(5 * time.Second)

			err = readUserRecommendations(db, cref, false, userId)
			if err != nil {
				log.Fatalln(err)
			}
		}
	}

	defer db.CloseConn()
}

func resetUserRecommmendations(db firestoreDB.FirestoreDB, cref string, userId string) error {
	fmt.Printf("Resetting user <%v> recommendations collection...", userId)
	err := db.DeleteCollection(cref, 20)
	return err
}

func readUserRecommendations(db firestoreDB.FirestoreDB, cref string, printDocuments bool, userId string) error {
	fmt.Printf("Reading user <%v> recommendations collection...", userId)
	err := db.ReadCollection(cref, false)
	return err
}

func writeUserRecommendationIds(db firestoreDB.FirestoreDB, documentName string, value *domain.Recommendations) error {
	err := db.SetDocument(documentName, value)
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
