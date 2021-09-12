/*
Copyright 2021 github.com/ucirello

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
package pglock_test

import (
	"context"
	"database/sql"
	"github.com/docker/go-connections/nat"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
	tc "github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"os"
	"testing"
	"time"
)

const (
	testDbUser     = "postgres"
	testDbPassword = "password"
	testDbDb       = "dev"
	testDbPort     = "5432"
)

type TestWithDB struct {
	suite.Suite
	db   tc.Container
	conn *sql.DB
	url  string

	createSQLFile string
}

func (s *TestWithDB) SetupSuite() {
	t := s.Suite.T()
	port := testDbPort + "/tcp"
	db := s.startDb(port, t)

	if ip, err := db.ContainerIP(context.Background()); err != nil {
		assert.NoError(t, err, "Failed to retrieve db test")
	} else {
		s.url = s.dbURL(ip)
	}
}

func (s *TestWithDB) dbURL(ip string) string {
	return "postgresql://" + testDbUser + ":" + testDbPassword + "@" + ip + ":" + testDbPort + "/" + testDbDb + "?sslmode=disable"
}

func (s *TestWithDB) startDb(port string, t *testing.T) tc.Container {
	db, err := tc.GenericContainer(context.Background(), tc.GenericContainerRequest{
		ContainerRequest: tc.ContainerRequest{
			Image: "postgres:9.6",
			Env: map[string]string{
				"POSTGRES_DB":       testDbDb,
				"POSTGRES_USER":     testDbUser,
				"POSTGRES_PASSWORD": testDbPassword,
			},
			ExposedPorts: []string{port},
			Cmd:          []string{testDbUser, "-c", "fsync=off"},
			WaitingFor:   wait.NewHostPortStrategy(nat.Port(port)).WithStartupTimeout(60 * time.Second),
			AutoRemove:   true,
		},
	})
	assert.NoError(t, err)
	assert.NoError(t, db.Start(context.Background()))
	return db
}

func TestWithDbTestSuite(t *testing.T) {
	suite.Run(t, new(TestWithDB))
}

func (s *TestWithDB) TearDownSuite() {
	if s.db != nil {
		_ = s.db.Terminate(context.Background())
	}
	//goland:noinspection ALL
	os.Remove(s.createSQLFile)
}
