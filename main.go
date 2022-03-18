package main

import (
	"bytes"
	"context"
	"encoding/csv"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"strconv"

	"github.com/jackc/pgx/v4"
)

const defaultMaxResponseSize = 1 << 20 // 1MB

var databaseUrl = os.Getenv("DATABASE_URL")

func query(w http.ResponseWriter, req *http.Request) {
	if req.Method != "POST" {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	contentType := req.Header.Get("Content-Type")
	// Assume text/sql if not included
	if contentType != "" && contentType != "text/sql" {
		w.WriteHeader(http.StatusUnsupportedMediaType)
		return
	}
	maxResponseSize := defaultMaxResponseSize
	headerMaxResponseSize, err := strconv.Atoi(req.Header.Get("Max-Content-Length"))
	if err != nil {
		maxResponseSize = headerMaxResponseSize
	}

	body, err := ioutil.ReadAll(req.Body)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	conn, err := pgx.Connect(context.Background(), databaseUrl)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	defer conn.Close(context.Background())

	rows, err := conn.Query(context.Background(), string(body))
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(err.Error()))
		return
	}
	defer rows.Close()

	buf := new(bytes.Buffer)
	csvWriter := csv.NewWriter(buf)
	currentRow := 0
	fields := rows.FieldDescriptions()

	colTypes := make([]string, len(fields))
	colNames := make([]string, len(fields))
	for i, field := range fields {
		colNames[i] = string(field.Name)
		dataType, ok := conn.ConnInfo().DataTypeForOID(field.DataTypeOID)
		if !ok {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		colTypes[i] = dataType.Name
	}
	coltypesBuf := new(bytes.Buffer)
	coltypesWriter := csv.NewWriter(coltypesBuf)
	if err := coltypesWriter.Write(colTypes); err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	coltypesWriter.Flush()
	w.Header().Add("Content-Type", "text/csv; coltypes="+coltypesBuf.String())
	csvWriter.Write(colNames)

	for rows.Next() {
		values, err := rows.Values()
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		records := make([]string, len(fields))
		for i, v := range values {
			records[i] = fmt.Sprintf("%v", v)
		}
		if err := csvWriter.Write(records); err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		currentRow++
		csvWriter.Flush()

		if buf.Len() > maxResponseSize {
			break
		}
	}
	if buf.Len() > maxResponseSize {
		w.Header().Set("Content-Range", "rows 0-"+strconv.Itoa(currentRow)+"/*")
		w.WriteHeader(http.StatusPartialContent)
	} else {
		w.Header().Set("Content-Range", "rows 0-"+strconv.Itoa(currentRow)+"/"+strconv.Itoa(currentRow))
	}

	cmdTag := rows.CommandTag()
	if cmdTag.Insert() || cmdTag.Update() || cmdTag.Delete() {
		w.Header().Set("Rows-Affected", strconv.FormatInt(cmdTag.RowsAffected(), 10))
	}

	buf.WriteTo(w)
}

func main() {
	http.HandleFunc("/query", query)
	fmt.Println("Server started on port 8090")
	log.Fatal(http.ListenAndServe(":8090", nil))
}
