package database

import (
	"crypto/sha1"
	"fmt"
)

func DatabaseHash(
	databaseUuid string,
	branchUuid string,
) string {
	sha1 := sha1.New()
	sha1.Write([]byte(fmt.Sprintf("%s:%s", databaseUuid, branchUuid)))

	return fmt.Sprintf("%x", sha1.Sum(nil))
}
