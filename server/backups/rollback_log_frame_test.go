package backups_test

import (
	"litebase/internal/test"
	"litebase/server/backups"
	"testing"
)

func TestRollbackFrame(t *testing.T) {
	test.Run(t, func() {
		frame := backups.RollbackLogFrame{
			Committed: 1,
			Offset:    100,
			Size:      200,
			Timestamp: 1234567890,
		}

		// Serialize the frame
		serialized, err := frame.Serialize()

		if err != nil {
			t.Fatalf("Failed to serialize RollbackLogFrame: %v", err)
		}

		// Deserialize the frame
		deserialized, err := backups.DeserializeRollbackLogFrame(serialized)

		if err != nil {
			t.Fatalf("Failed to deserialize RollbackLogFrame: %v", err)
		}

		// Check if the original frame and deserialized frame are equal
		if frame != deserialized {
			t.Fatalf("Expected deserialized frame to be %v, got %v", frame, deserialized)
		}
	})
}
