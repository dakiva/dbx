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
	"io/ioutil"
)

// An alias representing a query identifier. The purpose of this alias is to force authors to keep query names in sync in code with the underlying query names stored in the query json files. While technically a violation of DRY, it is good practice to store the query names in one place in code, rather than hardcoding them as strings. This makes the queries easier to search for, reuse, and ultimately remove.
type QueryIdentifier string

type QueryMap map[QueryIdentifier]QueryValue

// Finds and returns the query string for the given identifier. Returns a boolean flag indicating whether a valid query was found.
func (this QueryMap) Q(name QueryIdentifier) (string, bool) {
	if value, ok := this[name]; ok {
		return value.Query, ok
	}
	return "", false
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

// Used for unmarshalling the queries json object.
type QueryValue struct {
	Query       string
	Description string
}
