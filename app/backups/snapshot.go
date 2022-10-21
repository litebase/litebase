package backups

import (
	"bufio"
	"crypto/sha1"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

type Snapshot struct {
	branchUuid   string
	commits      []*Commit
	databaseUuid string
	Hash         string
	timestamp    int64
	pageHashes   []string

	StoresObjectHashes
}

func NewSnapshot(databaseUuid string, branchUuid string, timestamp int64, hash string) *Snapshot {
	if hash == "" {
		h := sha1.New()
		h.Write([]byte(fmt.Sprintf("%x", timestamp)))
		hash = fmt.Sprintf("%x", h.Sum(nil))
	}

	snapshot := &Snapshot{
		branchUuid:   branchUuid,
		databaseUuid: databaseUuid,
		Hash:         hash,
		timestamp:    timestamp,
	}

	if _, err := os.Stat(snapshot.GetPath(databaseUuid, branchUuid, timestamp, hash)); os.IsNotExist(err) {
		return nil
	}

	return snapshot
}

func (s *Snapshot) AddCommits(commits []*Commit) *Snapshot {
	commitHashses := make([]string, len(commits))

	for i, commit := range commits {
		commitHashses[i] = commit.Save().Key()
	}

	data := strings.Join(commitHashses, "\n")
	path := s.GetPath(s.databaseUuid, s.branchUuid, s.timestamp, s.Hash)
	var file *os.File

	// Check if file exists
	if _, err := os.Stat(filepath.Dir(path)); os.IsNotExist(err) {
		os.MkdirAll(filepath.Dir(path), 0755)
	}

	file, err := os.OpenFile(path, os.O_RDWR|os.O_APPEND, 0644)

	if err != nil {
		log.Fatal(err)
	}

	if _, err := os.Stat(path); err == nil {
		scanner := bufio.NewScanner(file)

		for scanner.Scan() {
			if scanner.Text() == s.Hash {
				return s
			}
		}
	}

	if _, err := file.Write([]byte(data)); err != nil {
		log.Fatal(err)
	}

	return s
}

func (s *Snapshot) GetCommits() []*Commit {
	if len(s.commits) == 0 {
		s.loadCommits()
	}

	return s.commits
}

func (s *Snapshot) GetObjectsForCommit(commit *Commit) []string {
	objects := map[string]int{}

	for _, c := range s.GetCommits() {
		for _, object := range c.GetObjects() {
			objects[object] = 0
		}

		if c.hash == commit.hash {
			break
		}
	}

	keys := make([]string, 0, len(objects))

	for k := range objects {
		keys = append(keys, k)
	}

	return keys
}

func (s *Snapshot) loadCommits() {
	path := s.GetPath(s.databaseUuid, s.branchUuid, s.timestamp, s.Hash)

	if _, err := os.Stat(filepath.Dir(path)); os.IsNotExist(err) {
		return
	}

	file, err := os.OpenFile(path, os.O_RDONLY, 0644)

	if err != nil {
		log.Fatal(err)
	}

	scanner := bufio.NewScanner(file)

	for scanner.Scan() {
		text := strings.Trim(scanner.Text(), " ")

		if text == "" {
			continue
		}

		key := strings.Split(text, ":")

		timestamp, _ := strconv.ParseInt(key[1], 10, 64)
		commitTimestamp, _ := strconv.ParseInt(key[2], 10, 64)

		s.commits = append(s.commits, NewCommit(
			s.databaseUuid,
			s.branchUuid,
			timestamp,
			commitTimestamp,
			key[0],
			[]string{},
		))
	}
}

func (s *Snapshot) WithCommits() *Snapshot {
	s.loadCommits()

	return s
}
