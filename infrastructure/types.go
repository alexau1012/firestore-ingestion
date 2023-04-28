package infrastructure

import "github.com/alexau1012/firestore-data-ingestion/domain"

type DocumentDatabase interface {
	ReadCollection(string, bool) error
	DeleteCollection(string) error
	SetDocument(string, *domain.Recommendations, string) error
	CloseConnection()
}
