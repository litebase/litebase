package sqlite3_test

// This file ensures database package CGO exports are linked when running sqlite3 tests.
// The database package provides CGO exports (goVectorScan, goVectorSearch, etc.) that
// are called by C code in vector_extension.c.  By importing database in the test package,
// we ensure these symbols are available to the linker without creating a circular dependency.

import (
	_ "github.com/litebase/litebase/pkg/database"
)
