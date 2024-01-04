package shrek

type ConsistencyLevel int

const (
	None ConsistencyLevel = iota
	Weak
	Strong
)

type ClusterState int

const (
	Leader ClusterState = iota
	Follower
	Candidate
	Shutdown
	Unknown
)

// BackupFormat defines the structure of a database backup
type BackupFormat int

const (
	BackupSQL BackupFormat = iota // plaintext SQL command format

	BackupBinary // SQLite file backup format
)

type payloadType int

const (
	Execute payloadType = iota //  Modifies the database
	Query                      // Queries the database
	Peer                       // Modifies the peers map
)
