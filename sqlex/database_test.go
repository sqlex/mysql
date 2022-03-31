package main

import "testing"

func TestDatabaseNew(t *testing.T) {
	database, err := NewMemoryDatabase()
	if err != nil {
		t.Error(err)
	}
	defer database.Close()
}
