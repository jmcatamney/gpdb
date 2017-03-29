package utils_test

import (
	. "backup_restore/utils"
	. "github.com/onsi/gomega"
	"testing"
	"time"
)

func TestCurrentTimestamp(t *testing.T) {
	RegisterTestingT(t)
	FPTimeNow = func() (time.Time) {return time.Date(2017, time.January, 1, 1, 1, 1, 1, time.Local)}
	expected := "20170101010101"
	actual := CurrentTimestamp()
	Expect(actual).To(Equal(expected))
}

func TestGetRows(t *testing.T) {
	RegisterTestingT(t)
	connection = &DBConn{Conn: nil, User: "", DBName: "", Host: "", Port: 0}
	connection.Conn.
