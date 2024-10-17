package storage_test

import (
	"litebase/internal/test"
	"litebase/server"
	"litebase/server/storage"
	"math/rand"
	"testing"

	"github.com/google/uuid"
)

func TestNewStorageHashRing(t *testing.T) {
	hashRing := storage.NewStorageNodeHashRing([]string{})

	if hashRing == nil {
		t.Fatal("Hash ring is nil")
	}
}

func TestStorageHashRingGetNode(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		hashRing := storage.NewStorageNodeHashRing([]string{"node1", "node2", "node3"})

		index, address, err := hashRing.GetNode("key1")

		if err != nil {
			t.Fatal(err)
		}

		if address == "" {
			t.Fatal("Node is empty")
		}

		if index < 0 {
			t.Fatal("Index is negative")
		}

		testCases := [][]string{}

		for i := 0; i < 100; i++ {
			randomNumber := rand.Intn(10000)
			keys := make([]string, randomNumber)

			for j := 0; j < randomNumber; j++ {
				keys[j] = uuid.New().String()
			}

			testCases = append(testCases, keys)
		}

		for _, keys := range testCases {
			// Ensure different keys are distributed to different nodes
			nodes := map[string]int{}

			for _, key := range keys {
				_, address, err = hashRing.GetNode(key)

				if err != nil {
					t.Fatal(err)
				}

				if address == "" {
					t.Fatal("Node is empty")
				}

				nodes[address]++
			}

			if len(nodes) != 3 {
				t.Error("Nodes are not distributed")
			}

			majorityThreshold := (len(keys) + 1) / 2
			majorityPercentage := (float64(majorityThreshold) / float64(len(keys))) * 100

			for node, count := range nodes {
				if count > majorityThreshold {
					t.Errorf(
						"node %s received more than %.2f%% of the keys with %d nodes: %d out of %d - %d%%",
						node, majorityPercentage, len(nodes), count, len(keys), count*100/len(keys),
					)
				}
			}
		}
	})
}

func TestStorageHashRingGetNodeEmpty(t *testing.T) {
	hashRing := storage.NewStorageNodeHashRing([]string{})

	_, _, err := hashRing.GetNode("key")

	if err == nil {
		t.Fatal("Error is nil")
	}

	if err != storage.ErrNoStorageNodesAvailable {
		t.Fatal("Error is not ErrNoStorageNodesAvailable")
	}
}

func TestStorageHashRingAddNode(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		hashRing := storage.NewStorageNodeHashRing([]string{"node1"})

		index, address, err := hashRing.GetNode("key1")

		if err != nil {
			t.Fatal(err)
		}

		if address == "" {
			t.Fatal("Node is empty")
		}

		if index < 0 {
			t.Fatal("Index is negative")
		}
	})
}

func TestStorageHashRingRemoveNode(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		hashRing := storage.NewStorageNodeHashRing([]string{"node1"})

		index, address, err := hashRing.GetNode("key1")

		if err != nil {
			t.Fatal(err)
		}

		if address == "" {
			t.Fatal("Node is empty")
		}

		if index < 0 {
			t.Fatal("Index is negative")
		}

		hashRing.RemoveNode("node1")

		index, address, err = hashRing.GetNode("key1")

		if err == nil {
			t.Fatal("Error is nil")
		}

		if address != "" {
			t.Fatal("Node is not empty")
		}

		if index != -1 {
			t.Fatal("Index is not negative")
		}
	})
}
