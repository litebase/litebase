package main

import (
	"encoding/json"
	"fmt"
	"log"

	"github.com/litebase/litebase/pkg/database"
)

func main() {
	// Test the DatabaseSettings JSON marshaling/unmarshaling
	settings := &database.DatabaseSettings{
		Backups: database.DatabaseBackupSettings{
			Enabled: true,
			IncrementalBackups: database.DatabaseIncrementalBackupSettings{
				Enabled: true,
			},
		},
	}

	// Test Value() method
	value, err := settings.Value()
	if err != nil {
		log.Fatal("Value() failed:", err)
	}
	fmt.Printf("JSON output: %s\n", value)

	// Test Scan() method
	var settings2 database.DatabaseSettings
	err = settings2.Scan(value)
	if err != nil {
		log.Fatal("Scan() failed:", err)
	}

	// Verify they're equal
	original, _ := json.Marshal(settings)
	reconstructed, _ := json.Marshal(&settings2)

	fmt.Printf("Original:      %s\n", original)
	fmt.Printf("Reconstructed: %s\n", reconstructed)

	if string(original) == string(reconstructed) {
		fmt.Println("✅ JSON storage/retrieval works correctly!")
	} else {
		fmt.Println("❌ JSON storage/retrieval failed!")
	}
}
