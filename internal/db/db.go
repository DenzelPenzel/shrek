package db

import (
	"database/sql/driver"
	"errors"
	"fmt"
	"github.com/mattn/go-sqlite3"
	"io"
	"os"
	"strings"
	"time"
)

type Queryer interface {
	Query(query string, args []driver.Value) (driver.Rows, error)
}

type Execer interface {
	Exec(query string, args []driver.Value) (driver.Result, error)
}

type DB struct {
	dbConn *sqlite3.SQLiteConn // Driver connection to database
	path   string              // Path to database file
}

type Result struct {
	LastInsertID int64   `json:"last_insert_id,omitempty"`
	RowsAffected int64   `json:"rows_affected,omitempty"`
	Error        string  `json:"error,omitempty"`
	Time         float64 `json:"time,omitempty"`
}

type Rows struct {
	Columns []string        `json:"columns,omitempty"`
	Types   []string        `json:"types,omitempty"`
	Values  [][]interface{} `json:"values,omitempty"`
	Error   string          `json:"error,omitempty"`
	Time    float64         `json:"time,omitempty"`
}

func New(fileName string) (*DB, error) {
	err := os.Remove(fileName)
	if err != nil {
		return nil, err
	}
	return Open(fileName)
}

func Open(path string) (*DB, error) {
	return open(formatDSNWithFilePath(path, ""))
}

func OpenWithDSN(path, dsn string) (*DB, error) {
	return open(formatDSNWithFilePath(path, dsn))
}

func OpenInMemoryWithDSN(dsn string) (*DB, error) {
	return open(formatDSNWithFilePath(":memory:", dsn))
}

func open(path string) (*DB, error) {
	d := sqlite3.SQLiteDriver{}
	dbc, err := d.Open(path)
	if err != nil {
		return nil, err
	}
	return &DB{
		dbConn: dbc.(*sqlite3.SQLiteConn),
		path:   path,
	}, nil
}

func (db *DB) Close() error {
	return db.dbConn.Close()
}

func (db *DB) Query(queries []string, useTx, includeTimings bool) ([]*Rows, error) {
	var queryer Queryer
	var transaction driver.Tx
	var res []*Rows

	err := func() error {
		var err error

		defer func() {
			if transaction != nil {
				if err != nil {
					err := transaction.Rollback()
					if err != nil {
						return
					}
					return
				}
				err := transaction.Commit()
				if err != nil {
					return
				}
			}
		}()

		queryer = db.dbConn

		if useTx {
			transaction, err = db.dbConn.Begin()
			if err != nil {
				return err
			}
		}

		for _, query := range queries {
			if query == "" {
				continue
			}

			rows := &Rows{}
			start := time.Now()

			response, err := queryer.Query(query, nil)
			if err != nil {
				rows.Error = err.Error()
				res = append(res, rows)
				continue
			}

			defer response.Close()

			columns := response.Columns()

			rows.Columns = columns
			rows.Types = response.(*sqlite3.SQLiteRows).DeclTypes()
			dest := make([]driver.Value, len(rows.Columns))

			for {
				err := response.Next(dest)
				if err != nil {
					if !errors.Is(err, io.EOF) {
						rows.Error = err.Error()
					}
					break
				}

				rows.Values = append(rows.Values, normalizeRowValues(dest, rows.Types))
			}

			if includeTimings {
				rows.Time = time.Since(start).Seconds()
			}

			res = append(res, rows)
		}

		return nil
	}()

	return res, err
}

// Backup ... consistent database snapshot
func (db *DB) Backup(path string) error {
	source, err := Open(path)
	if err != nil {
		return err
	}

	defer func(db *DB, err *error) {
		cerr := db.Close()
		if *err == nil {
			*err = cerr
		}
	}(source, &err)

	if err := copyDatabase(source.dbConn, db.dbConn); err != nil {
		return err
	}

	return err
}

// Execute ... TODO improve logic
func (db *DB) Execute(queries []string, useTx, includeTimings bool) ([]*Result, error) {
	var res []*Result
	var execer Execer
	var rollback bool
	var transaction driver.Tx

	err := func() error {
		var err error
		defer func() {
			if transaction != nil {
				if rollback {
					_ = transaction.Rollback()
					return
				}
				_ = transaction.Commit()
			}
		}()

		handleError := func(result *Result, err error) bool {
			result.Error = err.Error()
			res = append(res, result)
			if useTx {
				rollback = true // trigger the rollback
				return false
			}
			return true
		}

		execer = db.dbConn

		if useTx {
			transaction, err = db.dbConn.Begin()
			if err != nil {
				return err
			}
		}

		for _, query := range queries {
			if query == "" {
				continue
			}

			result := &Result{}
			startAt := time.Now()

			r, err := execer.Exec(query, nil)
			if err != nil {
				if handleError(result, err) {
					continue
				}
				break
			}

			if r == nil {
				continue
			}

			lastID, err := r.LastInsertId()
			if err != nil {
				if handleError(result, err) {
					continue
				}
				break
			}

			result.LastInsertID = lastID

			aRow, err := r.RowsAffected()
			if err != nil {
				if handleError(result, err) {
					continue
				}
				break
			}
			result.RowsAffected = aRow

			if includeTimings {
				result.Time = time.Since(startAt).Seconds()
			}

			res = append(res, result)
		}

		return nil
	}()

	return res, err
}

func copyDatabase(source *sqlite3.SQLiteConn, target *sqlite3.SQLiteConn) error {
	backup, err := source.Backup("main", target, "main")
	if err != nil {
		return err
	}

	for {
		done, err := backup.Step(-1)
		if err != nil {
			backupErr := backup.Finish()
			if backupErr != nil {
				return backupErr
			}
			return err
		}
		if done {
			break
		}
		time.Sleep(250 * time.Millisecond)
	}

	return backup.Finish()
}

func normalizeRowValues(row []driver.Value, types []string) []interface{} {
	values := make([]interface{}, len(types))
	for i, v := range row {
		if isTextType(types[i]) {
			switch val := v.(type) {
			case []byte:
				values[i] = string(val)
			default:
				values[i] = val
			}
		} else {
			values[i] = v
		}
	}
	return values
}

// isTextType returns whether the given type has a SQLite text affinity.
// http://www.sqlite.org/datatype3.html
func isTextType(t string) bool {
	return t == "text" ||
		t == "" ||
		strings.HasPrefix(t, "varchar") ||
		strings.HasPrefix(t, "varying character") ||
		strings.HasPrefix(t, "nchar") ||
		strings.HasPrefix(t, "native character") ||
		strings.HasPrefix(t, "nvarchar") ||
		strings.HasPrefix(t, "clob")
}

// formatDSNWithFilePath ... Format file path with data source name
func formatDSNWithFilePath(filePath, dataSourceName string) string {
	if dataSourceName != "" {
		return fmt.Sprintf("file:%s?%s", filePath, dataSourceName)
	}
	return filePath
}
