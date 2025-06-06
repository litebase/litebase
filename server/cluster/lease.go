package cluster

import (
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strconv"
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
	return time.Now().Unix() < l.ExpiresAt
}

// Check if the lease is expired based on the current time and the expiration time.
func (l *Lease) IsExpired() bool {
	if l.ExpiresAt == 0 {
		return false
	}

	return time.Now().Unix() > l.ExpiresAt
}

// Release the lease and remove the primary status from the node. This should
// be called before changing the cluster membership to replica.
func (l *Lease) Release() error {
	l.ExpiresAt = 0

	if l.node.Membership != ClusterMembershipPrimary {
		return fmt.Errorf("node is not a leader")
	}

	if err := l.node.truncateFile(l.node.Cluster.PrimaryPath()); err != nil {
		return err
	}

	if err := l.node.truncateFile(l.node.Cluster.LeasePath()); err != nil {
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

	expiresAt := time.Now().Add(LeaseDuration).Unix()
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

	l.RenewedAt = time.Now()
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
