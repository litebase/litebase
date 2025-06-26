package cluster_test

import (
	"testing"
	"time"

	"github.com/litebase/litebase/internal/test"
	"github.com/litebase/litebase/pkg/cluster"
	"github.com/litebase/litebase/pkg/server"
)

func TestLease(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		t.Run("NewLease", func(t *testing.T) {
			lease := cluster.NewLease(app.Cluster.Node())

			if lease == nil {
				t.Error("NewLease() returned nil")
			}
		})

		t.Run("IsUpToDate", func(t *testing.T) {
			lease := cluster.NewLease(app.Cluster.Node())
			lease.ExpiresAt = time.Now().UTC().Add(1 * time.Hour).Unix()

			if !lease.IsUpToDate() {
				t.Error("IsUpToDate() returned false for a valid lease")
			}
		})

		t.Run("IsExpired", func(t *testing.T) {
			lease := cluster.NewLease(app.Cluster.Node())
			lease.ExpiresAt = time.Now().UTC().Add(-1 * time.Hour).Unix()

			if !lease.IsExpired() {
				t.Error("IsExpired() returned false for an expired lease")
			}
		})
	})
}

func TestLease_Release(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		for {
			isPrimary := app.Cluster.Node().IsPrimary()
			if isPrimary {
				break
			}

			time.Sleep(100 * time.Millisecond)
		}

		lease := app.Cluster.Node().Lease()

		if err := lease.Release(); err != nil {
			t.Errorf("Release() returned error: %v", err)
		}

		if lease.ExpiresAt != 0 {
			t.Error("Release() did not reset ExpiresAt to 0")
		}
	})
}

func TestLease_Renew(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		for {
			isPrimary := app.Cluster.Node().IsPrimary()
			if isPrimary {
				break
			}

			time.Sleep(100 * time.Millisecond)
		}

		lease := app.Cluster.Node().Lease()

		if err := lease.Renew(); err != nil {
			t.Errorf("Renew() returned error: %v", err)
		}

		if lease.ExpiresAt <= 0 {
			t.Error("Renew() did not set a valid ExpiresAt")
		}
	})
}

func TestLease_Renew_Expired(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		for {
			isPrimary := app.Cluster.Node().IsPrimary()
			if isPrimary {
				break
			}

			time.Sleep(100 * time.Millisecond)
		}

		lease := app.Cluster.Node().Lease()
		lease.ExpiresAt = time.Now().UTC().Add(-1 * time.Hour).Unix()

		if err := lease.Renew(); err == nil {
			t.Error("Renew() did not return error for expired lease")
		}
	})
}

func TestLease_ShouldRenew(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		for {
			isPrimary := app.Cluster.Node().IsPrimary()
			if isPrimary {
				break
			}

			time.Sleep(100 * time.Millisecond)
		}

		lease := app.Cluster.Node().Lease()
		lease.RenewedAt = time.Now().UTC().Add(-cluster.LeaseDuration)

		if !lease.ShouldRenew() {
			t.Error("ShouldRenew() returned false for a valid lease")
		}

		lease.RenewedAt = time.Now().UTC().Add(cluster.LeaseDuration)

		if lease.ShouldRenew() {
			t.Error("ShouldRenew() returned true for a lease that should not renew")
		}
	})
}
