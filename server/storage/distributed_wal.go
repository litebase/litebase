package storage

/*
A DistributedWal is a write-ahead log that is distributed across multiple nodes.
The primary database can write to the DistributedWal to replicate changes to
the database replicas.
*/
type DistributedWal struct {
	BranchId   string
	DatabaseId string
}

type DistributedWalDistributor interface {
}

func NewDistributedWal() *DistributedWal {
	return &DistributedWal{}
}

/*
Close the DistributedWal.
*/
func (d *DistributedWal) Close() error {
	return nil
}

/*
Truncate the DistributedWal to the given size.
*/
func (d *DistributedWal) Truncate(size int64) error {
	return nil
}

/*
WriteAt writes len(p) bytes from p to the DistributedWal at the given offset.
*/
func (d *DistributedWal) WriteAt(p []byte, off int64) (n int, err error) {
	return 0, nil
}
