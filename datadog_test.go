package orm

import (
	"net/http"
	"testing"
	"time"

	apexLog "github.com/apex/log"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
)

func TestDataDog(t *testing.T) {
	engine := PrepareTables(t, &Registry{})

	apm := engine.DataDog().StartAPM("test_service", "test")
	assert.NotNil(t, apm)
	assert.Len(t, engine.dataDog.ctx, 1)
	engine.DataDog().SetAPMTag("test_tag", "hello")

	workSpan := engine.DataDog().StartWorkSpan("test_work")
	assert.NotNil(t, workSpan)
	assert.Len(t, engine.dataDog.ctx, 2)
	workSpan.SetTag("test_tag", "hello")
	workSpan.Finish()
	assert.Len(t, engine.dataDog.ctx, 1)

	request, _ := http.NewRequest(http.MethodGet, "/test/url/?ss=1&vv=2", nil)
	httpAPM := engine.DataDog().StartHTTPAPM(request, "test_service", "test")
	httpAPM.SetResponseStatus(http.StatusInternalServerError)
	assert.NotNil(t, httpAPM)
	httpAPM.Finish()

	engine.DataDog().EnableORMAPMLog(apexLog.DebugLevel, true)
	engine.DataDog().RegisterAPMError("test panic")
	engine.DataDog().RegisterAPMError(errors.Errorf("test error"))
	engine.DataDog().DropAPM()
	f := engine.DataDog().StartDataDogTracer(1)
	f()
	f2 := engine.DataDog().StartDataDogProfiler("test", "aa", "test", time.Minute)
	f2()
	apm.Finish()
}
