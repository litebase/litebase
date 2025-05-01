package storage

/*
The TieredFileSystemWal is a struct that represents a tiered file system
write-ahead log. This write-ahead log is used to store mutations made
to files in the tiered file system on the current node so they can
be recovered in the event of a failure.
*/
type TieredFileSystemWal struct{}
