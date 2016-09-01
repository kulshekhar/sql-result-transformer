package srt

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"testing"

	_ "github.com/lib/pq"
)

type (
	pgRow struct {
		IntField   int     `json:"intField"`
		TextField  string  `json:"textField"`
		BoolField  bool    `json:"boolField"`
		RealField  float64 `json:"realField"`
		JSONBField string  `json:"jsonbField"`
		JSONField  string  `json:"jsonField"`
	}
)

func TestPostgresJSON(t *testing.T) {
	query := "SELECT * FROM srt_test;"
	db := setup(
		"postgres",
		"user=test password=test dbname=test sslmode=disable",
		postgresSetupSQL,
	)

	jsonResult, err := GetTransformedResults(db, query, JSONTransformer{})
	if err != nil {
		t.Error(err)
	}
	defer teardown(db, postgresTeardownSQL)

	var pgResult []pgRow
	err = json.Unmarshal(jsonResult, &pgResult)
	if err != nil {
		t.Error(err)
	}
	if len(pgResult) != 4 {
		t.Errorf("Expected 4 rows. Got %d rows.\n", len(pgResult))
	}

	pgValidateRow1(t, pgResult[0])
	pgValidateRow2(t, pgResult[1])
	pgValidateRow3(t, pgResult[2])
	pgValidateRow4(t, pgResult[3])
}

func benchmarkPGJSON(b *testing.B, count int) {
	query := "SELECT * FROM srt_benchmark_test;"

	GetTransformedResults(pdbBM, query, JSONTransformer{})
}

func BenchmarkPGJSON(b *testing.B) {
	benchmarkPGJSON(b, bmCount)
}

func pgValidateRow1(t *testing.T, row pgRow) {
	jsonData := `{"id": 1, "name": "person 1"}`
	encodedJSON := base64.StdEncoding.EncodeToString([]byte(jsonData))
	validateField(t, 1, "boolField", row.BoolField, true)
	validateField(t, 1, "intField", row.IntField, 1)
	validateField(t, 1, "realField", row.RealField, 1.0)
	validateField(t, 1, "textField", row.TextField, "hello")
	validateField(t, 1, "jsonbField", row.JSONBField, encodedJSON)
	validateField(t, 1, "jsonField", row.JSONField, encodedJSON)
}

func pgValidateRow2(t *testing.T, row pgRow) {
	jsonData := `{"id": 2, "name": "person 2"}`
	encodedJSON := base64.StdEncoding.EncodeToString([]byte(jsonData))
	validateField(t, 2, "boolField", row.BoolField, false)
	validateField(t, 2, "intField", row.IntField, 2)
	validateField(t, 2, "realField", row.RealField, 0.0)
	validateField(t, 2, "textField", row.TextField, "world")
	validateField(t, 2, "jsonbField", row.JSONBField, encodedJSON)
	validateField(t, 2, "jsonField", row.JSONField, "")
}

func pgValidateRow3(t *testing.T, row pgRow) {
	jsonData := `{"id": 3, "name": "person 3"}`
	encodedJSON := base64.StdEncoding.EncodeToString([]byte(jsonData))
	validateField(t, 3, "boolField", row.BoolField, true)
	validateField(t, 3, "intField", row.IntField, 3)
	// converting float to string because floats are always off by some fraction
	validateField(t, 3, "realField", fmt.Sprintf("%.1f", row.RealField), "1.2")
	validateField(t, 3, "textField", row.TextField, "")
	validateField(t, 3, "jsonbField", row.JSONBField, "")
	validateField(t, 3, "jsonField", row.JSONField, encodedJSON)
}

func pgValidateRow4(t *testing.T, row pgRow) {
	jsonData := `{"id": 4, "name": "person 4"}`
	encodedJSON := base64.StdEncoding.EncodeToString([]byte(jsonData))
	validateField(t, 4, "boolField", row.BoolField, false)
	validateField(t, 4, "intField", row.IntField, 0)
	// converting float to string because floats are always off by some fraction
	validateField(t, 4, "realField", fmt.Sprintf("%.1f", row.RealField), "1.3")
	validateField(t, 4, "textField", row.TextField, "hello")
	validateField(t, 4, "jsonbField", row.JSONBField, encodedJSON)
	validateField(t, 4, "jsonField", row.JSONField, encodedJSON)
}

const (
	postgresTeardownSQL = `DROP TABLE IF EXISTS srt_test;`

	postgresSetupSQL = postgresTeardownSQL + `

CREATE TABLE srt_test
(
  intField int,
  textField text,
  boolField boolean,
  realField real,
  jsonbField jsonb,
  jsonField json
);
` + postgresInsertSQL

	postgresInsertSQL = `INSERT INTO srt_test
  (intField, textField, boolField, realField, jsonbField, jsonField)
VALUES
  (1, 'hello', true, 1.0, '{"id": 1, "name": "person 1"}', '{"id": 1, "name": "person 1"}'),
  (2, 'world', false, null, '{"id": 2, "name": "person 2"}', null),
  (3, null, true, 1.2, null, '{"id": 3, "name": "person 3"}'),
  (null, 'hello', null, 1.3, '{"id": 4, "name": "person 4"}', '{"id": 4, "name": "person 4"}');`

	postgresBenchmarkTeardownSQL = `DROP TABLE IF EXISTS srt_benchmark_test;`
	postgresBenchmarkSetupSQL    = postgresBenchmarkTeardownSQL + `
CREATE TABLE srt_benchmark_test
(
  intField int,
  textField text,
  boolField boolean,
  realField real,
  jsonbField jsonb,
  jsonField json
);
`
	postgresBenchmarkInsertSQL = `INSERT INTO srt_benchmark_test
  (intField, textField, boolField, realField, jsonbField, jsonField)
VALUES
  (1, 'hello', true, 1.0, '{"id": 1, "name": "person 1"}', '{"id": 1, "name": "person 1"}'),
  (2, 'world', false, null, '{"id": 2, "name": "person 2"}', null),
  (3, null, true, 1.2, null, '{"id": 3, "name": "person 3"}'),
  (null, 'hello', null, 1.3, '{"id": 4, "name": "person 4"}', '{"id": 4, "name": "person 4"}');`
)
