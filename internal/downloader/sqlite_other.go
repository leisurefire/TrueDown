//go:build !windows

package downloader

import "fmt"

type sqliteConn struct{}
type sqliteRows struct{}

func openSQLite(string) (*sqliteConn, error) {
	return nil, fmt.Errorf("TrueDown's embedded SQLite store currently requires Windows winsqlite3.dll")
}

func (c *sqliteConn) Close() error                       { return nil }
func (c *sqliteConn) Exec(string, ...any) (int64, error) { return 0, fmt.Errorf("sqlite unavailable") }
func (c *sqliteConn) Query(string, ...any) (*sqliteRows, error) {
	return nil, fmt.Errorf("sqlite unavailable")
}
func (r *sqliteRows) Next() (bool, error) { return false, nil }
func (r *sqliteRows) Text(int) string     { return "" }
func (r *sqliteRows) Int64(int) int64     { return 0 }
func (r *sqliteRows) IsNull(int) bool     { return true }
func (r *sqliteRows) Close()              {}
