package connections

const (
	BalanceThreshold = 25
)

/*
A connection balance is a counter that tracks the number of connections
attempts that are made per database within a threshold. This is used to
*/
type ConnectionBalance struct {
	Balance      int
	branchUuid   string
	databaseUuid string
	RequestCount int
}

/*
Create a new instance of the connection balance.
*/
func NewConnectionBalance(databaseUuid, branchUuid string) *ConnectionBalance {
	return &ConnectionBalance{
		Balance:      0,
		branchUuid:   branchUuid,
		databaseUuid: databaseUuid,
		RequestCount: 0,
	}
}

/*
Consume an attempt on the counter.
*/
func (c *ConnectionBalance) Consume() {
	c.Balance--

	if c.Balance < 0 {
		c.Reset()
	}
}

/*
Check if the balance is below the threshold.
*/
func (c *ConnectionBalance) IsNegative() bool {
	return c.Balance <= 0
}

/*
Reset the counter.
*/
func (c *ConnectionBalance) Reset() {
	c.Balance = 0
}

/*
Increase the request count and increase the balance if the request count
exceeds the balance threshold.
*/
func (c *ConnectionBalance) Tick() {
	c.RequestCount++

	if c.RequestCount >= BalanceThreshold {
		c.Balance++
		c.RequestCount = 0
	}
}
