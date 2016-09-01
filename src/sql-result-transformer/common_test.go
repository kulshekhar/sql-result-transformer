package srt

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"strconv"
	"testing"

	_ "github.com/lib/pq"
	_ "github.com/mattn/go-sqlite3"
)

var (
	pdbBM    *sql.DB
	sqliteBM *sql.DB

	bmCount int
)

func init() {
	bmCount, _ = strconv.Atoi(os.Getenv("count"))
	if bmCount < 1 {
		bmCount = 1
	}
}

func TestMain(m *testing.M) {
	setupBenchmarkDatabases()

	fmt.Printf("Benchmarking with %d rows\n", bmCount*4)

	result := m.Run()

	teardownBenchmarkDatabases()

	os.Exit(result)
}

func setupBenchmarkDatabases() {
	pdbBM = setup(
		"postgres",
		"user=test password=test dbname=test sslmode=disable",
		postgresBenchmarkSetupSQL,
	)
	os.Remove("/tmp/srt_benchmark.db")
	sqliteBM = setup(
		"sqlite3",
		"/tmp/srt_benchmark.db",
		sqliteSetupSQL,
	)
	insertData()
}

func insertData() {
	pdbBM.Exec("BEGIN TRANSACTION;")
	sqliteBM.Exec("BEGIN TRANSACTION;")
	for i := 0; i < bmCount; i++ {
		pdbBM.Exec(postgresBenchmarkInsertSQL)
		sqliteBM.Exec(sqliteInsertSQL)
	}
	pdbBM.Exec("COMMIT;")
	sqliteBM.Exec("COMMIT;")
}

func teardownBenchmarkDatabases() {
	teardown(pdbBM, postgresBenchmarkTeardownSQL)
	teardown(sqliteBM, sqliteTeardownSQL)
	os.Remove("/tmp/srt_benchmark.db")
}

func validateField(t *testing.T, rowNum int, fieldName string, actual, expected interface{}) {
	fail := true
	switch expected.(type) {
	case []uint8:
		if a, ok := actual.([]uint8); ok {
			if e, ok := expected.([]uint8); ok {
				if len(a) == len(e) {
					if len(a) == 0 {
						fail = false
					}
					for i := range a {
						if a[i] != e[i] {
							fail = true
							break
						}
						fail = false
					}
				}
			}
		}
	default:
		if expected == actual {
			fail = false
		}
	}
	if fail {
		t.Errorf("In Row %d: Expected %s to be %v. Got %v", rowNum, fieldName, expected, actual)
	}
}

func setup(dbType, connectionString, sqlDDL string) *sql.DB {
	db, err := sql.Open(dbType, connectionString)
	if err != nil {
		log.Fatal(err)
	}

	if _, err = db.Exec(sqlDDL); err != nil {
		log.Fatal(err)
	}
	return db
}

func teardown(db *sql.DB, sqlDDL string) {
	if db != nil {
		defer db.Close()

		if _, err := db.Exec(sqlDDL); err != nil {
			log.Fatal(err)
		}
	}
}
