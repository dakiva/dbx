// Copyright 2014 Daniel Akiva

// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at

// http://www.apache.org/licenses/LICENSE-2.0

// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package dbx

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
)

type QueryMap map[string]QueryValue

// Finds and returns the query string for the given identifier. Panics if a query was not found.
func (this QueryMap) Q(name string) string {
	if value, ok := this[name]; ok {
		return value.Query
	}
	panic(fmt.Sprintf("Could not find a query for name: %v", name))
}

// Loads named queries from explicit file locations, returning an error if a file could not be loaded or parsed as JSON. The JSON format is simply { "queryName", { "query" : "SELECT * FROM...", "description": "A select statement" }. If two queries have the same name either in the same file, or in disparate files, the last query loaded wins, overwriting the previously loaded query.
func LoadNamedQueries(fileLocations ...string) (QueryMap, error) {
	queryMap := make(QueryMap)
	for _, location := range fileLocations {
		bytes, err := ioutil.ReadFile(location)
		if err != nil {
			return nil, err
		}
		err = json.Unmarshal(bytes, &queryMap)
		if err != nil {
			return nil, err
		}
	}
	return queryMap, nil
}

// Variant of LoadNamedQueries that panics if an error occurs while loading the queries.
func MustLoadNamedQueries(fileLocations ...string) QueryMap {
	queryMap, err := LoadNamedQueries(fileLocations...)
	if err != nil {
		panic(fmt.Sprintf("Error loading named queries: %v", err))
	}
	return queryMap
}

// Used for unmarshalling the queries json object.
type QueryValue struct {
	Query       string
	Description string
}
