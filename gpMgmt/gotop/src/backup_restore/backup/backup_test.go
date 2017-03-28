package backup_test

import (
	"backup_restore/utils"
	. "github.com/onsi/gomega"
	"testing"
)

func TestCurrentTimestamp(t *testing.T) {
	RegisterTestingT(t)
	expected := ""
	actual := utils.CurrentTimestamp()
	Expect(actual).To(Equal(expected))
}
