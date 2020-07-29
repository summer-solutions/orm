package orm

import (
	"net/http"
	"net/url"
	"os"
	"testing"

	log2 "github.com/apex/log"
	"github.com/apex/log/handlers/memory"
	"github.com/juju/errors"

	"github.com/stretchr/testify/assert"
)

func TestLogger(t *testing.T) {
	registry := &Registry{}
	validated, _ := registry.Validate()
	engine := validated.CreateEngine()
	logger := memory.New()
	engine.EnableLogger(log2.WarnLevel, logger)
	engine.Log().AddFields(log2.Fields{"a": "test", "b": "test2"})
	engine.Log().Warn("test warn", log2.Fields{"c": "test3"})
	assert.Len(t, logger.Entries, 1)
	assert.Len(t, logger.Entries[0].Fields, 5)
	assert.Equal(t, "test", logger.Entries[0].Fields["a"])
	assert.Equal(t, "test2", logger.Entries[0].Fields["b"])
	assert.Equal(t, "test3", logger.Entries[0].Fields["c"])
	assert.Equal(t, "github.com/summer-solutions/orm", logger.Entries[0].Fields["logger.name"])
	assert.Equal(t, log2.WarnLevel, logger.Entries[0].Level)
	assert.Equal(t, "test warn", logger.Entries[0].Message)

	engine.Log().ErrorMessage("test error", log2.Fields{"c": "test4", "d": "test5"})
	assert.Len(t, logger.Entries, 2)
	assert.Len(t, logger.Entries[1].Fields, 6)
	assert.Equal(t, "test", logger.Entries[1].Fields["a"])
	assert.Equal(t, "test2", logger.Entries[1].Fields["b"])
	assert.Equal(t, "test4", logger.Entries[1].Fields["c"])
	assert.Equal(t, "test5", logger.Entries[1].Fields["d"])
	assert.Equal(t, log2.ErrorLevel, logger.Entries[1].Level)

	engine.Log().Debug("test debug", log2.Fields{"c": "test4"})
	assert.Len(t, logger.Entries, 2)
	engine.Log().Info("test debug", log2.Fields{"c": "test4"})
	assert.Len(t, logger.Entries, 2)

	err := func() error {
		return errors.Trace(errors.Trace(errors.New("test error")))
	}()

	engine.Log().SetHTTPResponseCode(500)
	request := &http.Request{Method: http.MethodPost, Host: "test_domain", URL: &url.URL{Path: "/test/"}}
	engine.Log().AddFieldsFromHTTPRequest(request, "123.11.23.12")
	engine.Log().Error(err, log2.Fields{"c": "test4"})
	assert.Len(t, logger.Entries, 3)
	assert.Len(t, logger.Entries[2].Fields, 17)
	assert.Equal(t, "test", logger.Entries[2].Fields["a"])
	assert.Equal(t, "test2", logger.Entries[2].Fields["b"])
	assert.Equal(t, "test4", logger.Entries[2].Fields["c"])
	assert.Equal(t, "*errors.Err", logger.Entries[2].Fields["error.kind"])
	assert.Equal(t, "test error", logger.Entries[2].Fields["error.message"])
	assert.Equal(t, "test_domain", logger.Entries[2].Fields["http.host"])
	assert.Equal(t, "POST", logger.Entries[2].Fields["http.method"])
	assert.Equal(t, 500, logger.Entries[2].Fields["http.status_code"])
	assert.Equal(t, "123.11.23.12", logger.Entries[2].Fields["network.client.ip"])

	engine.Log().Error("test error", log2.Fields{"c": "test4"})
	assert.Len(t, logger.Entries, 4)
	assert.Len(t, logger.Entries[3].Fields, 17)
	assert.Equal(t, "panicRecovery", logger.Entries[3].Fields["error.kind"])

	os.Stderr = nil
	engine.EnableLogger(log2.WarnLevel)
	engine.Log().Warn("test warn", log2.Fields{"c": "test3"})
}
