package srt

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"os"
	"testing"
)

type (
	slRow struct {
		IntField  int     `json:"intField"`
		TextField string  `json:"textField"`
		BoolField bool    `json:"boolField"`
		RealField float64 `json:"realField"`
		BlobField []byte  `json:"blobField"`
	}
)

func TestSqliteJSON(t *testing.T) {
	query := "SELECT * FROM srt_test;"
	setupSQLite()
	db := setup(
		"sqlite3",
		"/tmp/srt.db",
		sqliteSetupSQL,
	)

	jsonResult, err := GetTransformedResults(db, query, JSONTransformer{})
	if err != nil {
		t.Error(err)
	}
	defer teardownSQLite()

	var slResult []slRow
	err = json.Unmarshal(jsonResult, &slResult)
	if err != nil {
		t.Error(err)
	}
	if len(slResult) != 4 {
		t.Errorf("Expected 4 rows. Got %d rows.\n", len(slResult))
	}

	slValidateRow1(t, slResult[0])
	slValidateRow2(t, slResult[1])
	slValidateRow3(t, slResult[2])
	slValidateRow4(t, slResult[3])
}

func benchmarkSqliteJSON(b *testing.B, count int) {
	query := "SELECT * FROM srt_test;"

	GetTransformedResults(sqliteBM, query, JSONTransformer{})
}

func BenchmarkSqliteJSON(b *testing.B) {
	benchmarkSqliteJSON(b, bmCount)
}

func slValidateRow1(t *testing.T, row slRow) {
	validateField(t, 1, "intField", row.IntField, 1)
	validateField(t, 1, "textField", row.TextField,
		base64.StdEncoding.EncodeToString([]byte("hello")))
	validateField(t, 1, "boolField", row.BoolField, true)
	// converting float to string because floats are always off by some fraction
	validateField(t, 1, "realField", fmt.Sprintf("%.1f", row.RealField), "1.0")
	validateField(t, 1, "blobField", row.BlobField, blobArray)
}

func slValidateRow2(t *testing.T, row slRow) {
	validateField(t, 2, "intField", row.IntField, 2)
	validateField(t, 2, "textField", row.TextField,
		base64.StdEncoding.EncodeToString([]byte("world")))
	validateField(t, 2, "boolField", row.BoolField, false)
	// converting float to string because floats are always off by some fraction
	validateField(t, 2, "realField", fmt.Sprintf("%.1f", row.RealField), "0.0")
	validateField(t, 2, "blobField", row.BlobField, blobArray)
}

func slValidateRow3(t *testing.T, row slRow) {
	validateField(t, 3, "intField", row.IntField, 3)
	validateField(t, 3, "textField", row.TextField, "")
	validateField(t, 3, "boolField", row.BoolField, false)
	// converting float to string because floats are always off by some fraction
	validateField(t, 3, "realField", fmt.Sprintf("%.1f", row.RealField), "1.2")
	validateField(t, 3, "blobField", row.BlobField, []uint8{})
}

func slValidateRow4(t *testing.T, row slRow) {
	validateField(t, 4, "intField", row.IntField, 0)
	validateField(t, 4, "textField", row.TextField,
		base64.StdEncoding.EncodeToString([]byte("hello")))
	validateField(t, 4, "boolField", row.BoolField, true)
	// converting float to string because floats are always off by some fraction
	validateField(t, 4, "realField", fmt.Sprintf("%.1f", row.RealField), "1.3")
	validateField(t, 4, "blobField", row.BlobField, blobArray)
}

func setupSQLite() {
	os.Remove(sqliteFile)
}

func teardownSQLite() {
	os.Remove(sqliteFile)
}

var blobArray = []uint8{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}

const (
	sqliteFile = "/tmp/srt.db"

	sqliteTeardownSQL = `DROP TABLE IF EXISTS srt_test;`

	sqliteSetupSQL = sqliteTeardownSQL + `

CREATE TABLE srt_test
(
  intField int,
  textField text,
  boolField boolean,
  realField real,
  blobField blob
);

` + sqliteInsertSQL

	sqliteInsertSQL = `INSERT INTO srt_test
  (intField, textField, boolField, realField, blobField)
VALUES
  (1, 'hello', 1, 1.0, X'0102030405060708090a0b0c0d0e0f'),
  (2, 'world', 0, null, X'0102030405060708090a0b0c0d0e0f'),
  (3, null, null, 1.2, null),
  (null, 'hello', 1, 1.3, X'0102030405060708090a0b0c0d0e0f');`
)
