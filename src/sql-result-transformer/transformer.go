package srt

import "database/sql"

// ITransformer should be implemented by any type that wants to transform
// the results of an SQL query
type ITransformer interface {
	Transform(result *[]map[string]interface{}) ([]byte, error)
}

// GetTransformedResults executes the `selectQuery` using `db` and uses the
// `transformer` to return the processed result of the query
func GetTransformedResults(
	db *sql.DB,
	selectQuery string,
	transformer ITransformer,

) ([]byte, error) {

	mapList, err := GetMapList(db, selectQuery)
	if err != nil {
		return nil, err
	}

	return transformer.Transform(mapList)
}

// GetMapList executes the `selectQuery` using `db` and returns the result
// as an array of maps
func GetMapList(
	db *sql.DB,
	selectQuery string,
) (*[]map[string]interface{}, error) {

	rows, err := db.Query(selectQuery)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	columns, _ := rows.Columns()
	columnCount := len(columns)

	// create a map with column names as keys (this will hold one row)
	// create an array whose elements will be pointers to the map values
	tmpRowMap := map[string]*interface{}{}
	tmpRowHolder := make([]interface{}, columnCount)

	for i := 0; i < columnCount; i++ {
		var tmp interface{}
		tmpRowMap[columns[i]] = &tmp
		tmpRowHolder[i] = &tmp
	}

	var result []map[string]interface{}

	for rows.Next() {

		// use tmpRowHolder to hold the values of the row
		// these values can also be accessed using tmpRowMap
		err = rows.Scan(tmpRowHolder...)
		if err != nil {
			return nil, err
		}

		// create a new row map and store the dereferenced values from
		// the temporary row map in it
		row := map[string]interface{}{}
		for k, v := range tmpRowMap {
			// switch t := (*v).(type) {
			// default:
			// 	fmt.Printf("%s (%T) : %v\n", k, t, *v)
			// }
			row[k] = *v
		}

		result = append(result, row)
	}

	return &result, nil
}
