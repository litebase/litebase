package cluster

import (
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"syscall"
	"time"
)

var (
	ErrLeaseExpired = errors.New("lease expired")
)

type Lease struct {
	node      *Node
	ExpiresAt int64
	RenewedAt time.Time
}

func NewLease(node *Node) *Lease {
	return &Lease{
		node: node,
	}
}

// Check if the lease is up to date based on the current time and the expiration time.
func (l *Lease) IsUpToDate() bool {
	return time.Now().UTC().Unix() < l.ExpiresAt
}

// Check if the lease is expired based on the current time and the expiration time.
func (l *Lease) IsExpired() bool {
	if l.ExpiresAt == 0 {
		return false
	}

	return time.Now().UTC().Unix() > l.ExpiresAt
}

// Release the lease and remove the primary status from the node. This should
// be called before changing the cluster membership to replica.
func (l *Lease) Release() error {
	l.ExpiresAt = 0

	if l.node.Membership != ClusterMembershipPrimary {
		return fmt.Errorf("node is not a leader")
	}

	// Lock the file, read it, and verify the address matches before truncating
	// it. This will prevent a node from accidentally removing the primary file.
	primaryFile, err := l.node.Cluster.NetworkFS().OpenFile(l.node.Cluster.PrimaryPath(), os.O_RDWR, 0600)

	if err != nil {
		slog.Debug("Failed to open primary file", "error", err)
		return err
	}

	// Attempt to lock the primary file using syscall.Flock
	file := primaryFile.(*os.File)

	locked := false

	defer func() {
		if locked {
			err = syscall.Flock(int(file.Fd()), syscall.LOCK_UN)

			if err != nil {
				slog.Debug("Failed to unlock primary file", "error", err)
			}
		}

		err = primaryFile.Close()

		if err != nil {
			slog.Debug("Failed to close primary file", "error", err)
		}
	}()

	err = syscall.Flock(int(file.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)

	if err != nil {
		if errors.Is(err, syscall.EWOULDBLOCK) || errors.Is(err, syscall.EAGAIN) {
			slog.Debug("Primary file is locked by another process", "error", err)
			return nil // Not an error, just not elected
		}

		return err
	}

	var primaryData []byte
	primaryData = make([]byte, 256)

	for {
		n, err := file.Read(primaryData)
		if err != nil {
			return err
		}

		primaryData = primaryData[:n]

		if n < 256 {
			break
		}
	}

	if string(primaryData) != l.node.address {
		return fmt.Errorf("primary address verification failed")
	}

	if err := primaryFile.Truncate(0); err != nil {
		return err
	}

	// Lock the lease file, read it, and verify the address matches before truncating
	// it. This will prevent a node from accidentally removing the lease file.

	leaseFile, err := l.node.Cluster.NetworkFS().OpenFile(l.node.Cluster.LeasePath(), os.O_RDWR, 0600)

	if err != nil {
		slog.Debug("Failed to open lease file", "error", err)
		return err
	}

	// Attempt to lock the lease file using syscall.Flock
	defer func() {
		if locked {
			err = syscall.Flock(int(leaseFile.(*os.File).Fd()), syscall.LOCK_UN)

			if err != nil {
				slog.Debug("Failed to unlock lease file", "error", err)
			}
		}

		err = leaseFile.Close()

		if err != nil {
			slog.Debug("Failed to close lease file", "error", err)
		}
	}()

	err = syscall.Flock(int(leaseFile.(*os.File).Fd()), syscall.LOCK_EX|syscall.LOCK_NB)

	if err != nil {
		if errors.Is(err, syscall.EWOULDBLOCK) || errors.Is(err, syscall.EAGAIN) {
			slog.Debug("Lease file is locked by another process", "error", err)
			return nil // Not an error, just not elected
		}

		return err
	}

	if err := leaseFile.Truncate(0); err != nil {
		return err
	}

	return nil
}

func (l *Lease) Renew() error {
	address, _ := l.node.Address()

	if l.IsExpired() {
		slog.Error("Lease expired, cannot renew", "expires_at", l.ExpiresAt)
		return ErrLeaseExpired
	}

	if l.node.Membership != ClusterMembershipPrimary {
		return fmt.Errorf("node is not a leader")
	}

	if err := l.node.context.Err(); err != nil {
		slog.Debug("Operation canceled before starting")
		return err
	}

	// Verify the primary is stil the current node
	primaryAddress, err := l.node.Cluster.NetworkFS().ReadFile(l.node.Cluster.PrimaryPath())

	if err != nil {
		return err
	}

	if string(primaryAddress) != address {
		l.node.SetMembership(ClusterMembershipReplica)

		return fmt.Errorf("primary address verification failed")
	}

	if err := l.node.context.Err(); err != nil {
		slog.Debug("Operation canceled before starting")
		return err
	}

	expiresAt := time.Now().UTC().Add(LeaseDuration).Unix()
	leaseTimestamp := strconv.FormatInt(expiresAt, 10)

	err = l.node.Cluster.NetworkFS().WriteFile(l.node.Cluster.LeasePath(), []byte(leaseTimestamp), os.ModePerm)

	if err != nil {
		slog.Error("Failed to write lease file", "error", err)
		return err
	}

	if err := l.node.context.Err(); err != nil {
		slog.Debug("Operation canceled before starting")
		return err
	}

	// Verify the Lease file has the written value
	leaseData, err := l.node.Cluster.NetworkFS().ReadFile(l.node.Cluster.LeasePath())

	if err != nil {
		slog.Error("Failed to read lease file", "error", err)
		return err
	}

	if string(leaseData) != leaseTimestamp {
		return fmt.Errorf("failed to verify lease file")
	}

	l.RenewedAt = time.Now().UTC()
	l.ExpiresAt = expiresAt

	return nil
}

// Determine if the lease should be renewed based on the remaining time.
func (l *Lease) ShouldRenew() bool {
	if l.IsExpired() {
		return false
	}

	return (LeaseDuration - time.Since(l.RenewedAt)) < 10*time.Second
}
