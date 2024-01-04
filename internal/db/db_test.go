package db

import (
	"encoding/json"
	"github.com/stretchr/testify/require"
	"os"
	"path"
	"testing"
)

func TestDatabase(t *testing.T) {
	t.Run("test database file creation", func(t *testing.T) {
		dir, err := os.MkdirTemp("", "tmp-db-test")
		require.NoError(t, err)

		defer func(path string) {
			err := os.RemoveAll(path)
			require.NoError(t, err)
		}(dir)

		db, err := Open(path.Join(dir, "test_db"))
		require.NoError(t, err)
		require.NotNil(t, db)
		err = db.Close()
		require.NoError(t, err)
	})

	t.Run("test database table creation", func(t *testing.T) {
		db, path := openDB(t)
		defer db.Close()
		defer os.Remove(path)

		_, err := db.Execute([]string{"create table test (id integer not null primary key, name TEXT)"}, false, false)
		require.NoError(t, err)

		r, err := db.Query([]string{"select * from test"}, false, false)
		require.NoError(t, err)

		res, err := json.Marshal(r)
		require.NoError(t, err)
		require.Equal(t, `[{"columns":["id","name"],"types":["integer","text"]}]`, string(res))
	})

	t.Run("test empty statement", func(t *testing.T) {
		db, path := openDB(t)
		defer db.Close()
		defer os.Remove(path)

		_, err := db.Execute([]string{"create table test (id integer not null primary key, name TEXT)"}, false, false)
		require.NoError(t, err)

		_, err = db.Execute([]string{""}, false, false)
		require.NoError(t, err)

		_, err = db.Execute([]string{";"}, false, false)
		require.NoError(t, err)
	})

	t.Run("test master statement", func(t *testing.T) {
		db, path := openDB(t)
		defer db.Close()
		defer os.Remove(path)

		_, err := db.Execute([]string{"create table test (id integer not null primary key, name text)"}, false, false)
		require.NoError(t, err)

		r, err := db.Query([]string{"select * from sqlite_master"}, false, false)
		require.NoError(t, err)
		res, err := json.Marshal(r)
		require.NoError(t, err)
		require.Equal(t, `[{"columns":["type","name","tbl_name","rootpage","sql"],"types":["text","text","text","int","text"],"values":[["table","test","test",2,"CREATE TABLE test (id integer not null primary key, name text)"]]}]`, string(res))
	})

	t.Run("test insert, delete, select execution", func(t *testing.T) {
		db, path := openDB(t)
		defer db.Close()
		defer os.Remove(path)

		_, err := db.Execute([]string{"create table test (id integer not null primary key, name text)"}, false, false)
		require.NoError(t, err)

		// insert new record
		_, err = db.Execute([]string{`insert into test(name) values("ana")`}, false, false)
		require.NoError(t, err)

		// select table
		raw, err := db.Query([]string{"select * from test"}, false, false)
		require.NoError(t, err)
		res, err := json.Marshal(raw)
		require.NoError(t, err)
		require.Equal(t, `[{"columns":["id","name"],"types":["integer","text"],"values":[[1,"ana"]]}]`, string(res))

		// insert new record
		_, err = db.Execute([]string{`insert into test(name) values("lenny")`}, false, false)
		require.NoError(t, err)

		// select table after insert
		raw, err = db.Query([]string{"select * from test"}, false, false)
		require.NoError(t, err)
		res, err = json.Marshal(raw)
		require.NoError(t, err)
		require.Equal(t, `[{"columns":["id","name"],"types":["integer","text"],"values":[[1,"ana"],[2,"lenny"]]}]`, string(res))

		// select from table where name="ana", after insert
		raw, err = db.Query([]string{`select * from test where name="ana"`}, false, false)
		require.NoError(t, err)
		res, err = json.Marshal(raw)
		require.NoError(t, err)
		require.Equal(t, `[{"columns":["id","name"],"types":["integer","text"],"values":[[1,"ana"]]}]`, string(res))

		// select from table when record not exist
		raw, err = db.Query([]string{`select * from test where name="petya"`}, false, false)
		require.NoError(t, err)
		res, err = json.Marshal(raw)
		require.NoError(t, err)
		require.Equal(t, `[{"columns":["id","name"],"types":["integer","text"]}]`, string(res))

		// update record
		_, err = db.Execute([]string{`update test SET Name='vasya' where id=2`}, false, false)
		require.NoError(t, err)

		raw, err = db.Query([]string{"select * from test"}, false, false)
		require.NoError(t, err)
		res, err = json.Marshal(raw)
		require.NoError(t, err)
		require.Equal(t, `[{"columns":["id","name"],"types":["integer","text"],"values":[[1,"ana"],[2,"vasya"]]}]`, string(res))

		// delete
		_, err = db.Execute([]string{"DELETE FROM test WHERE Id=2"}, false, false)
		require.NoError(t, err)

		raw, err = db.Query([]string{"SELECT * FROM test"}, false, false)
		require.NoError(t, err)
		res, err = json.Marshal(raw)
		require.NoError(t, err)
		require.Equal(t, `[{"columns":["id","name"],"types":["integer","text"],"values":[[1,"ana"]]}]`, string(res))
	})

	t.Run("test insert, delete, select failed cases", func(t *testing.T) {
		db, path := openDB(t)
		defer db.Close()
		defer os.Remove(path)

		q, err := db.Query([]string{"SELECT * FROM test"}, false, false)
		require.NoError(t, err)
		res, err := json.Marshal(q)
		require.NoError(t, err)
		require.Equal(t, `[{"error":"no such table: test"}]`, string(res))

		e, err := db.Execute([]string{`insert into test(name) values("ana")`}, false, false)
		require.NoError(t, err)
		res, err = json.Marshal(e)
		require.NoError(t, err)
		require.Equal(t, `[{"error":"no such table: test"}]`, string(res))

		// duplicate table
		_, err = db.Execute([]string{"create table test (id integer not null primary key, name text)"}, false, false)
		require.NoError(t, err)
		e, err = db.Execute([]string{"create table test (id integer not null primary key, name text)"}, false, false)
		require.NoError(t, err)
		res, err = json.Marshal(e)
		require.NoError(t, err)
		require.Equal(t, `[{"error":"table test already exists"}]`, string(res))

		_, err = db.Execute([]string{`insert into test(id, name) values(1, "ana")`}, false, false)
		require.NoError(t, err)

		e, err = db.Execute([]string{`insert into test(id, name) values(1, "ana")`}, false, false)
		require.NoError(t, err)
		res, err = json.Marshal(e)
		require.NoError(t, err)
		require.Equal(t, `[{"error":"UNIQUE constraint failed: test.id"}]`, string(res))

		e, err = db.Execute([]string{"test test"}, false, false)
		require.NoError(t, err)
		res, err = json.Marshal(e)
		require.NoError(t, err)
		require.Equal(t, `[{"error":"near \"test\": syntax error"}]`, string(res))
	})
}

func openDB(t *testing.T) (*DB, string) {
	f, err := os.CreateTemp("", "shrek_test")
	require.NoError(t, err)

	f.Close()

	db, err := Open(f.Name())
	require.NoError(t, err)

	return db, f.Name()
}
