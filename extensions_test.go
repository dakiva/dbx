// Copyright 2019 Daniel Akiva

// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at

// http://www.apache.org/licenses/LICENSE-2.0

// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// +build extensions

package dbx

import (
	"testing"

	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/assert"
)

func TestInstallExtensionsWithNoFile(t *testing.T) {
	// given
	migrationsDir := "db/extNoFile"
	pgdsn := GetTestDsn()
	db, err := sqlx.Connect(pgType, pgdsn)
	assert.NoError(t, err)
	defer db.Close()

	// when
	err = InstallExtensions("public", migrationsDir, db)

	// then
	assert.NoError(t, err)
}

func TestRemoveExtensionsWithNoFile(t *testing.T) {
	// given
	migrationsDir := "db/extNoFile"
	pgdsn := GetTestDsn()
	db, err := sqlx.Connect(pgType, pgdsn)
	assert.NoError(t, err)
	defer db.Close()

	// when
	err = RemoveExtensions(migrationsDir, db)

	// then
	assert.NoError(t, err)
}

func TestExtensions(t *testing.T) {
	// given
	migrationsDir := "db/extGoodFile"
	pgdsn := GetTestDsn()
	db, err := sqlx.Connect(pgType, pgdsn)
	assert.NoError(t, err)
	defer db.Close()

	// when
	err = InstallExtensions("public", migrationsDir, db)

	// then
	assert.NoError(t, err)
	err = RemoveExtensions(migrationsDir, db)
	assert.NoError(t, err)
}

func TestGetExtensions(t *testing.T) {
	// given
	migrationsDir := "db/extGoodFile"

	// when
	extensions, err := getExtensions(migrationsDir)

	// then
	assert.NoError(t, err)
	assert.Len(t, extensions, 1)
	assert.Equal(t, "pg_trgm", extensions[0])
}
