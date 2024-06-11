package backups

import (
	"bufio"
	"crypto/sha1"
	"fmt"
	"litebase/server/storage"
	"strings"
)

type Commit struct {
	DatabaseUuid    string `json:"databaseUuid"`
	BranchUuid      string `json:"branchUuid"`
	Timestamp       int64  `json:"timestamp"`
	CommitTimestamp int64  `json:"commitTimestamp"`
	Hash            string `json:"hash"`
	objectHashes    []string

	StoresObjectHashes
}

func NewCommit(
	databaseUuid string,
	branchUuid string,
	timestamp int64,
	commitTimestamp int64,
	hash string,
	objectHashes []string,
) *Commit {
	return &Commit{
		DatabaseUuid:    databaseUuid,
		BranchUuid:      branchUuid,
		Timestamp:       timestamp,
		CommitTimestamp: commitTimestamp,
		Hash:            hash,
		objectHashes:    objectHashes,
	}
}

func (c *Commit) GetObjects() []string {
	if len(c.objectHashes) == 0 {
		c.loadObjects()
	}

	return c.objectHashes
}

func (c *Commit) Key() string {
	return fmt.Sprintf("%s:%d:%d", c.Hash, c.Timestamp, c.CommitTimestamp)
}

func (c *Commit) loadObjects() {
	file, err := storage.FS().Open(c.GetPath(c.DatabaseUuid, c.BranchUuid, c.Timestamp, c.Hash))

	if err != nil {
		return
	}

	defer file.Close()

	scanner := bufio.NewScanner(file)

	for scanner.Scan() {
		c.objectHashes = append(c.objectHashes, strings.Trim(scanner.Text(), " "))
	}
}

func (c *Commit) Save() *Commit {
	keys := make(map[string]bool)
	list := []string{}
	for _, entry := range c.objectHashes {
		if _, value := keys[entry]; !value {
			keys[entry] = true
			list = append(list, entry)
		}
	}

	data := strings.Join(list, "\n")
	hash := sha1.New()
	hash.Write([]byte(data))
	hashString := fmt.Sprintf("%x", hash.Sum(nil))

	c.Hash = hashString
	path := c.GetPath(c.DatabaseUuid, c.BranchUuid, c.Timestamp, c.Hash)
	c.storeObjectHash(path, []byte(data))

	return c
}
