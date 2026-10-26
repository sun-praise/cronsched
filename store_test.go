package cronsched_test

import (
	"testing"

	"github.com/sun-praise/cronsched"
	"github.com/sun-praise/cronsched/storetest"
)

func TestMemStoreContract(t *testing.T) {
	storetest.Contract(t, func(t *testing.T) cronsched.Store { return cronsched.NewMemStore() })
}
