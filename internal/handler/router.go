package handler

import (
	"io"
	"net/http"

	"github.com/gorilla/mux"
	"github.com/sirupsen/logrus"
	"github.com/spacelift-io/homework-object-storage/internal/core"
	"github.com/spacelift-io/homework-object-storage/internal/core/distributor"
)

func Router(objectDistributor *distributor.ObjectDistributor) http.Handler {
	r := mux.NewRouter()
	r.HandleFunc("/object/{id:[a-zA-Z0-9]{1,32}}", putObject(objectDistributor)).Methods(http.MethodPut)
	r.HandleFunc("/object/{id:[a-zA-Z0-9]{1,32}}", getObject(objectDistributor)).Methods(http.MethodGet)
	return r
}

func putObject(objectDistributor *distributor.ObjectDistributor) func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		objectID := mux.Vars(r)["id"]

		bodyBytes, err := io.ReadAll(r.Body)
		if err != nil {
			logrus.WithFields(logrus.Fields{
				"id": objectID,
			}).WithError(err).Error("reading request body")
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		if err := objectDistributor.PutObject(r.Context(), objectID, bodyBytes); err != nil {
			logrus.WithFields(logrus.Fields{
				"id": objectID,
			}).WithError(err).Error("putting object")
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		w.WriteHeader(http.StatusNoContent)
	}
}

func getObject(objectDistributor *distributor.ObjectDistributor) func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		objectID := mux.Vars(r)["id"]
		object, err := objectDistributor.GetObject(r.Context(), objectID)
		switch err {
		case nil:
		// Ok
		case core.ErrNotFound:
			w.WriteHeader(http.StatusNotFound)
			return
		default:
			logrus.WithFields(logrus.Fields{
				"id": objectID,
			}).WithError(err).Error("getting object")
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		if _, err := w.Write(object); err != nil {
			logrus.WithFields(logrus.Fields{
				"id": objectID,
			}).WithError(err).Error("writing response")
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
	}
}
