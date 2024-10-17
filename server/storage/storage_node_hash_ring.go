package storage

import (
	"fmt"
	"hash/crc32"
	"sort"
	"sync"
)

/*
A hash ring implementation for storage nodes. This is used to distribute the data
across the storage nodes in a cluster.
*/
type StorageNodeHashRing struct {
	nodes      []string
	hashMap    map[uint32]string
	sortedKeys []uint32
	mutex      sync.RWMutex
}

/*
Create a new storage node hash ring with the given nodes.
*/
func NewStorageNodeHashRing(nodes []string) *StorageNodeHashRing {
	hr := &StorageNodeHashRing{
		hashMap: make(map[uint32]string),
		mutex:   sync.RWMutex{},
		nodes:   nodes,
	}

	hr.generateHashMap()

	return hr
}

/*
Generate the hash map for the storage nodes.
*/
func (hr *StorageNodeHashRing) generateHashMap() {
	hr.hashMap = make(map[uint32]string)
	hr.sortedKeys = []uint32{}

	if len(hr.nodes) == 0 {
		return
	}

	currentVirtualNodes := len(hr.nodes)

	// Determine the scaling factor to get as close to 100 as possible
	scalingFactor := 100 / float64(currentVirtualNodes)

	// Apply the scaling factor
	virtualNodes := int(float64(len(hr.nodes))*scalingFactor) * len(hr.nodes)

	for _, node := range hr.nodes {
		for i := 0; i < virtualNodes; i++ {
			// Create a unique virtual node hash by combining node name and index
			virtualNode := fmt.Sprintf("%s#%d", node, i)
			hash := crc32.ChecksumIEEE([]byte(virtualNode))
			hr.hashMap[hash] = node
			hr.sortedKeys = append(hr.sortedKeys, hash)
		}
	}

	sort.Slice(hr.sortedKeys, func(i, j int) bool {
		return hr.sortedKeys[i] < hr.sortedKeys[j]
	})
}

/*
Get the storage node for the given key.
*/
func (hr *StorageNodeHashRing) GetNode(key string) (int, string, error) {
	hr.mutex.RLock()
	defer hr.mutex.RUnlock()

	if len(hr.sortedKeys) == 0 {
		return -1, "", ErrNoStorageNodesAvailable
	}

	// Ensure the key starts with a slash to increase the distribution of keys
	if key[0] != '/' {
		key = "/" + key
	}

	// Get the numerical hash of the key
	hash := crc32.ChecksumIEEE([]byte(key))

	idx := sort.Search(len(hr.sortedKeys), func(i int) bool {
		return hr.sortedKeys[i] >= hash
	})

	if idx == len(hr.sortedKeys) {
		idx = 0
	}

	return idx, hr.hashMap[hr.sortedKeys[idx]], nil
}

/*
Add a new storage node to the hash ring.
*/
func (hr *StorageNodeHashRing) AddNode(node string) {
	hr.mutex.Lock()
	defer hr.mutex.Unlock()

	for _, n := range hr.nodes {
		if n == node {
			return
		}
	}

	hr.nodes = append(hr.nodes, node)
	hr.generateHashMap()
}

/*
Remove a storage node from the hash ring.
*/
func (hr *StorageNodeHashRing) RemoveNode(node string) {
	hr.mutex.Lock()
	defer hr.mutex.Unlock()

	for i, n := range hr.nodes {
		if n == node {
			hr.nodes = append(hr.nodes[:i], hr.nodes[i+1:]...)
			break
		}
	}

	hr.generateHashMap()
}
