//go:build windows

package downloader

import (
	"fmt"
	"sync"
	"syscall"
	"unsafe"
)

const (
	sqliteOK            = 0
	sqliteRow           = 100
	sqliteDone          = 101
	sqliteOpenReadWrite = 0x00000002
	sqliteOpenCreate    = 0x00000004
	sqliteOpenFullMutex = 0x00010000
)

var (
	sqliteDLL          = syscall.NewLazyDLL("winsqlite3.dll")
	sqliteOpenV2       = sqliteDLL.NewProc("sqlite3_open_v2")
	sqliteCloseV2      = sqliteDLL.NewProc("sqlite3_close_v2")
	sqlitePrepareV2    = sqliteDLL.NewProc("sqlite3_prepare_v2")
	sqliteStep         = sqliteDLL.NewProc("sqlite3_step")
	sqliteFinalize     = sqliteDLL.NewProc("sqlite3_finalize")
	sqliteBindText     = sqliteDLL.NewProc("sqlite3_bind_text")
	sqliteBindInt64    = sqliteDLL.NewProc("sqlite3_bind_int64")
	sqliteBindNull     = sqliteDLL.NewProc("sqlite3_bind_null")
	sqliteColumnText   = sqliteDLL.NewProc("sqlite3_column_text")
	sqliteColumnInt64  = sqliteDLL.NewProc("sqlite3_column_int64")
	sqliteColumnType   = sqliteDLL.NewProc("sqlite3_column_type")
	sqliteErrmsg       = sqliteDLL.NewProc("sqlite3_errmsg")
	sqliteLastInsertID = sqliteDLL.NewProc("sqlite3_last_insert_rowid")
)

type sqliteConn struct {
	mu     sync.Mutex
	handle uintptr
}

type sqliteRows struct {
	conn *sqliteConn
	stmt uintptr
}

func openSQLite(path string) (*sqliteConn, error) {
	cPath, err := syscall.BytePtrFromString(path)
	if err != nil {
		return nil, err
	}
	var handle uintptr
	rc, _, _ := sqliteOpenV2.Call(
		uintptr(unsafe.Pointer(cPath)),
		uintptr(unsafe.Pointer(&handle)),
		sqliteOpenReadWrite|sqliteOpenCreate|sqliteOpenFullMutex,
		0,
	)
	if int32(rc) != sqliteOK {
		if handle != 0 {
			sqliteCloseV2.Call(handle)
		}
		return nil, fmt.Errorf("sqlite open failed (%d)", int32(rc))
	}
	return &sqliteConn{handle: handle}, nil
}

func (c *sqliteConn) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.handle == 0 {
		return nil
	}
	rc, _, _ := sqliteCloseV2.Call(c.handle)
	if int32(rc) != sqliteOK {
		return c.error(int32(rc))
	}
	c.handle = 0
	return nil
}

func (c *sqliteConn) Exec(query string, args ...any) (int64, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	stmt, err := c.prepare(query, args)
	if err != nil {
		return 0, err
	}
	defer sqliteFinalize.Call(stmt)
	for {
		rc, _, _ := sqliteStep.Call(stmt)
		if int32(rc) == sqliteDone {
			break
		}
		if int32(rc) != sqliteRow {
			return 0, c.error(int32(rc))
		}
	}
	id, _, _ := sqliteLastInsertID.Call(c.handle)
	return int64(id), nil
}

func (c *sqliteConn) Query(query string, args ...any) (*sqliteRows, error) {
	c.mu.Lock()
	stmt, err := c.prepare(query, args)
	if err != nil {
		c.mu.Unlock()
		return nil, err
	}
	return &sqliteRows{conn: c, stmt: stmt}, nil
}

func (c *sqliteConn) prepare(query string, args []any) (uintptr, error) {
	cQuery, err := syscall.BytePtrFromString(query)
	if err != nil {
		return 0, err
	}
	var stmt uintptr
	rc, _, _ := sqlitePrepareV2.Call(
		c.handle,
		uintptr(unsafe.Pointer(cQuery)),
		uintptr(^uint32(0)),
		uintptr(unsafe.Pointer(&stmt)),
		0,
	)
	if int32(rc) != sqliteOK {
		return 0, c.error(int32(rc))
	}
	for i, arg := range args {
		if err := c.bind(stmt, i+1, arg); err != nil {
			sqliteFinalize.Call(stmt)
			return 0, err
		}
	}
	return stmt, nil
}

func (c *sqliteConn) bind(stmt uintptr, index int, value any) error {
	var rc uintptr
	switch v := value.(type) {
	case nil:
		rc, _, _ = sqliteBindNull.Call(stmt, uintptr(index))
	case string:
		p, err := syscall.BytePtrFromString(v)
		if err != nil {
			return err
		}
		rc, _, _ = sqliteBindText.Call(stmt, uintptr(index), uintptr(unsafe.Pointer(p)), uintptr(^uint32(0)), ^uintptr(0))
	case int:
		rc, _, _ = sqliteBindInt64.Call(stmt, uintptr(index), uintptr(int64(v)))
	case int64:
		rc, _, _ = sqliteBindInt64.Call(stmt, uintptr(index), uintptr(v))
	default:
		return fmt.Errorf("unsupported sqlite argument %T", value)
	}
	if int32(rc) != sqliteOK {
		return c.error(int32(rc))
	}
	return nil
}

func (c *sqliteConn) error(code int32) error {
	ptr, _, _ := sqliteErrmsg.Call(c.handle)
	return fmt.Errorf("sqlite error %d: %s", code, cString(ptr))
}

func (r *sqliteRows) Next() (bool, error) {
	rc, _, _ := sqliteStep.Call(r.stmt)
	switch int32(rc) {
	case sqliteRow:
		return true, nil
	case sqliteDone:
		return false, nil
	default:
		return false, r.conn.error(int32(rc))
	}
}

func (r *sqliteRows) Text(column int) string {
	ptr, _, _ := sqliteColumnText.Call(r.stmt, uintptr(column))
	return cString(ptr)
}

func (r *sqliteRows) Int64(column int) int64 {
	v, _, _ := sqliteColumnInt64.Call(r.stmt, uintptr(column))
	return int64(v)
}

func (r *sqliteRows) IsNull(column int) bool {
	v, _, _ := sqliteColumnType.Call(r.stmt, uintptr(column))
	return v == 5
}

func (r *sqliteRows) Close() {
	if r.stmt != 0 {
		sqliteFinalize.Call(r.stmt)
		r.stmt = 0
		r.conn.mu.Unlock()
	}
}

//go:nocheckptr
func cString(ptr uintptr) string {
	if ptr == 0 {
		return ""
	}
	const maxCString = 64 << 20
	bytes := unsafe.Slice((*byte)(unsafe.Pointer(ptr)), maxCString)
	for i, b := range bytes {
		if b == 0 {
			return string(bytes[:i])
		}
	}
	return ""
}
