package srt

import "encoding/json"

// JSONTransformer implements ITransformer to return the resuest of the SELECT
// query in the JSON format
type JSONTransformer struct{}

// Transform Converts the query result into the JSON format
func (t JSONTransformer) Transform(
	result *[]map[string]interface{},
) ([]byte, error) {

	return json.Marshal(*result)

}
