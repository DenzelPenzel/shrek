package shrek

import (
	"bytes"
	"encoding/binary"
	sql "github.com/draculaas/shrek/internal/db"
	"github.com/hashicorp/raft"
)

type fsmExecuteResponse struct {
	results []*sql.Result
	error   error
}

type fsmQueryResponse struct {
	rows  []*sql.Rows
	error error
}

type fsmGenericResponse struct {
	error error
}

type fsmSnapshot struct {
	database []byte
	meta     []byte
}

// Persist writes the snapshot to the given sink
func (f *fsmSnapshot) Persist(sink raft.SnapshotSink) error {
	err := func() error {
		// Start by writing size of database
		b := new(bytes.Buffer)
		size := uint64(len(f.database))
		err := binary.Write(b, binary.LittleEndian, size)
		if err != nil {
			return err
		}
		if _, err := sink.Write(b.Bytes()); err != nil {
			return err
		}

		if _, err := sink.Write(f.database); err != nil {
			return err
		}

		if _, err := sink.Write(f.meta); err != nil {
			return err
		}

		return sink.Close()
	}()

	if err != nil {
		sink.Cancel()
		return err
	}

	return nil
}

func (f *fsmSnapshot) Release() {}
