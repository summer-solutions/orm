package orm

import (
	"fmt"
	"net/http"
	"net/url"
	"testing"
	"time"

	apexLog "github.com/apex/log"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
)

func TestDataDog(t *testing.T) {
	engine := PrepareTables(t, &Registry{})

	engine.DataDog().StartAPM("test_service", "test")
	assert.Len(t, engine.dataDog.ctx, 1)
	engine.DataDog().SetAPMTag("test_tag", "hello")

	workSpan := engine.DataDog().StartWorkSpan("test_work")
	assert.NotNil(t, workSpan)
	assert.Len(t, engine.dataDog.ctx, 2)
	workSpan.SetTag("test_tag", "hello")
	workSpan.Finish()
	assert.Len(t, engine.dataDog.ctx, 1)

	request, _ := http.NewRequest(http.MethodPost, "/test/url/?ss=1&vv=2", nil)
	request.PostForm = url.Values{"ss": []string{"a"}}
	httpAPM := engine.DataDog().StartHTTPAPM(request, "test_service", "test")
	httpAPM.SetResponseStatus(http.StatusInternalServerError)
	assert.NotNil(t, httpAPM)
	httpAPM.Finish()

	for i := 1; i <= 100000; i++ {
		request.PostForm[fmt.Sprintf("s_%d", i)] = []string{"hello world"}
	}
	httpAPM = engine.DataDog().StartHTTPAPM(request, "test_service", "test")
	httpAPM.SetResponseStatus(http.StatusInternalServerError)
	assert.NotNil(t, httpAPM)
	httpAPM.Finish()

	engine.DataDog().EnableORMAPMLog(apexLog.DebugLevel, true)
	engine.DataDog().RegisterAPMError("test panic")
	engine.DataDog().RegisterAPMError(errors.Errorf("test error"))
	engine.DataDog().DropAPM()
	f := StartDataDogTracer(1)
	f()
	_ = StartDataDogProfiler("test", "aa", "test", time.Minute)
	//f2() //TODO check why it's blocking system
	engine.DataDog().FinishAPM()
}
