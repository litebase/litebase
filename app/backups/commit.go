package backups

import (
	"bufio"
	"crypto/sha1"
	"fmt"
	"os"
	"strings"
)

type Commit struct {
	databaseUuid    string
	branchUuid      string
	timestamp       int
	commitTimestamp int
	hash            string
	objectHashes    []string

	StoresObjectHashes
}

func NewCommit(databaseUuid string, branchUuid string, timestamp int, commitTimestamp int, hash string, objectHashes []string) *Commit {
	return &Commit{
		databaseUuid:    databaseUuid,
		branchUuid:      branchUuid,
		timestamp:       timestamp,
		commitTimestamp: commitTimestamp,
		hash:            hash,
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
	return fmt.Sprintf("%s:%d:%d", c.hash, c.timestamp, c.commitTimestamp)
}

func (c *Commit) loadObjects() {
	file, err := os.Open(c.GetPath(c.databaseUuid, c.branchUuid, c.timestamp, c.hash))

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

	path := c.GetPath(c.databaseUuid, c.branchUuid, c.timestamp, c.hash)
	c.storeObjectHash(path, []byte(data))
	c.hash = hashString

	return c
}
