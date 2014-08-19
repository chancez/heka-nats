package nats

import (
	"github.com/rafrombrc/gospec/src/gospec"
	"testing"
)

func TestAllSpecs(t *testing.T) {
	r := gospec.NewRunner()
	r.Parallel = false

	r.AddSpec(NatsInputSpec)

	gospec.MainGoTest(r, t)
}
