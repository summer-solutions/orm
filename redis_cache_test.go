package orm

import (
	"testing"
	"time"

	"github.com/go-redis/redis/v8"

	apexLog "github.com/apex/log"
	"github.com/apex/log/handlers/memory"

	"github.com/go-redis/redis_rate/v9"

	"github.com/stretchr/testify/assert"
)

func TestRedis(t *testing.T) {
	registry := &Registry{}
	registry.RegisterRedis("localhost:6381", 15)
	registry.RegisterRedisStream("test-stream", "default", []string{"test-group"})
	registry.RegisterRedisStream("test-stream-a", "default", []string{"test-group"})
	registry.RegisterRedisStream("test-stream-b", "default", []string{"test-group"})
	validatedRegistry, err := registry.Validate()
	assert.Nil(t, err)
	engine := validatedRegistry.CreateEngine()
	testRedis(t, engine)

	registry = &Registry{}
	registry.RegisterRedis("localhost:6389", 15)
	validatedRegistry, err = registry.Validate()
	assert.NoError(t, err)
	engine = validatedRegistry.CreateEngine()
	testLogger := memory.New()
	engine.AddQueryLogger(testLogger, apexLog.InfoLevel, QueryLoggerSourceRedis)
	assert.Panics(t, func() {
		engine.GetRedis().Get("invalid")
	})
}

func testRedis(t *testing.T, engine *Engine) {
	r := engine.GetRedis()

	testLogger := memory.New()
	engine.AddQueryLogger(testLogger, apexLog.InfoLevel, QueryLoggerSourceRedis)
	r.FlushDB()
	testLogger.Entries = make([]*apexLog.Entry, 0)

	assert.True(t, r.RateLimit("test", redis_rate.PerSecond(2)))
	assert.True(t, r.RateLimit("test", redis_rate.PerSecond(2)))
	assert.False(t, r.RateLimit("test", redis_rate.PerSecond(2)))
	assert.Len(t, testLogger.Entries, 3)

	valid := false
	val := r.GetSet("test_get_set", 10, func() interface{} {
		valid = true
		return "ok"
	})
	assert.True(t, valid)
	assert.Equal(t, "ok", val)
	valid = false
	val = r.GetSet("test_get_set", 10, func() interface{} {
		valid = true
		return "ok"
	})
	assert.False(t, valid)
	assert.Equal(t, "ok", val)

	val, has := r.Get("test_get")
	assert.False(t, has)
	assert.Equal(t, "", val)
	r.Set("test_get", "hello", 1)
	val, has = r.Get("test_get")
	assert.True(t, has)
	assert.Equal(t, "hello", val)

	r.LPush("test_list", "a")
	assert.Equal(t, int64(1), r.LLen("test_list"))
	r.RPush("test_list", "b", "c")
	assert.Equal(t, int64(3), r.LLen("test_list"))
	assert.Equal(t, []string{"a", "b", "c"}, r.LRange("test_list", 0, 2))
	assert.Equal(t, []string{"b", "c"}, r.LRange("test_list", 1, 5))
	r.LSet("test_list", 1, "d")
	assert.Equal(t, []string{"a", "d", "c"}, r.LRange("test_list", 0, 2))
	r.LRem("test_list", 1, "c")
	assert.Equal(t, []string{"a", "d"}, r.LRange("test_list", 0, 2))

	val, has = r.RPop("test_list")
	assert.True(t, has)
	assert.Equal(t, "d", val)
	r.Ltrim("test_list", 1, 2)
	val, has = r.RPop("test_list")
	assert.False(t, has)
	assert.Equal(t, "", val)

	r.HSet("test_map", "name", "Tom")
	assert.Equal(t, map[string]string{"name": "Tom"}, r.HGetAll("test_map"))
	v, has := r.HGet("test_map", "name")
	assert.True(t, has)
	assert.Equal(t, "Tom", v)
	_, has = r.HGet("test_map", "name2")
	assert.False(t, has)

	r.HSet("test_map", "last", "Summer", "age", "16")
	assert.Equal(t, map[string]string{"age": "16", "last": "Summer", "name": "Tom"}, r.HGetAll("test_map"))
	assert.Equal(t, map[string]interface{}{"age": "16", "missing": nil, "name": "Tom"}, r.HMget("test_map",
		"name", "age", "missing"))

	added := r.ZAdd("test_z", &redis.Z{Member: "a", Score: 10}, &redis.Z{Member: "b", Score: 20})
	assert.Equal(t, int64(2), added)
	assert.Equal(t, []string{"b", "a"}, r.ZRevRange("test_z", 0, 3))
	assert.Equal(t, float64(10), r.ZScore("test_z", "a"))
	resZRange := r.ZRangeWithScores("test_z", 0, 3)
	assert.Len(t, resZRange, 2)
	assert.Equal(t, "a", resZRange[0].Member)
	assert.Equal(t, "b", resZRange[1].Member)
	assert.Equal(t, float64(10), resZRange[0].Score)
	assert.Equal(t, float64(20), resZRange[1].Score)
	resZRange = r.ZRevRangeWithScores("test_z", 0, 3)
	assert.Len(t, resZRange, 2)
	assert.Equal(t, "b", resZRange[0].Member)
	assert.Equal(t, "a", resZRange[1].Member)
	assert.Equal(t, float64(20), resZRange[0].Score)
	assert.Equal(t, float64(10), resZRange[1].Score)

	assert.Equal(t, int64(2), r.ZCard("test_z"))
	assert.Equal(t, int64(2), r.ZCount("test_z", "10", "20"))
	assert.Equal(t, int64(1), r.ZCount("test_z", "11", "20"))
	r.Del("test_z")
	assert.Equal(t, int64(0), r.ZCount("test_z", "10", "20"))

	r.MSet("key_1", "a", "key_2", "b")
	assert.Equal(t, map[string]interface{}{"key_1": "a", "key_2": "b", "missing": nil}, r.MGet("key_1", "key_2", "missing"))

	added = r.SAdd("test_s", "a", "b", "c", "d", "a")
	assert.Equal(t, int64(4), added)
	assert.Equal(t, int64(4), r.SCard("test_s"))
	val, has = r.SPop("test_s")
	assert.NotEqual(t, "", val)
	assert.True(t, has)
	assert.Len(t, r.SPopN("test_s", 10), 3)
	assert.Len(t, r.SPopN("test_s", 10), 0)
	val, has = r.SPop("test_s")
	assert.Equal(t, "", val)
	assert.False(t, has)

	id := engine.GetEventBroker().PublishMap("test-stream", EventAsMap{"name": "a", "code": "b"})
	assert.NotEmpty(t, id)
	assert.Equal(t, int64(1), r.XLen("test-stream"))
	assert.Equal(t, int64(1), r.XTrim("test-stream", 0, false))
	assert.Equal(t, int64(0), r.XLen("test-stream"))

	engine.GetEventBroker().PublishMap("test-stream", EventAsMap{"name": "a1", "code": "b1"})
	engine.GetEventBroker().PublishMap("test-stream", EventAsMap{"name": "a2", "code": "b2"})
	engine.GetEventBroker().PublishMap("test-stream", EventAsMap{"name": "a3", "code": "b3"})
	engine.GetEventBroker().PublishMap("test-stream", EventAsMap{"name": "a4", "code": "b4"})
	engine.GetEventBroker().PublishMap("test-stream", EventAsMap{"name": "a5", "code": "b5"})
	res, has := r.XGroupCreate("test-stream", "test-group", "0")
	assert.Equal(t, "OK", res)
	assert.False(t, has)
	assert.Equal(t, int64(1), r.Exists("test-stream"))
	assert.Equal(t, "stream", r.Type("test-stream"))
	res, has = r.XGroupCreate("test-stream", "test-group", "0")
	assert.True(t, has)
	assert.Equal(t, "OK", res)
	res, has = r.XGroupCreateMkStream("test-stream-2", "test-group-2", "$")
	assert.False(t, has)
	assert.Equal(t, "OK", res)
	assert.Equal(t, int64(1), r.Exists("test-stream-2"))
	res, has = r.XGroupCreateMkStream("test-stream-2", "test-group-2", "$")
	assert.True(t, has)
	assert.Equal(t, "OK", res)
	assert.Equal(t, int64(1), r.Exists("test-stream-2"))
	deleted := r.XGroupDestroy("test-stream-2", "test-group-2")
	assert.Equal(t, int64(1), deleted)
	assert.Equal(t, int64(0), r.Exists("test-group-2"))
	info := r.XInfoStream("test-stream")
	assert.Equal(t, int64(1), info.Groups)
	infoGroups := r.XInfoGroups("test-stream")
	assert.Len(t, infoGroups, 1)
	assert.Equal(t, "test-group", infoGroups[0].Name)
	assert.Equal(t, "0-0", infoGroups[0].LastDeliveredID)
	assert.Equal(t, int64(0), infoGroups[0].Consumers)
	assert.Equal(t, int64(0), infoGroups[0].Pending)
	streams := r.XReadGroup(&redis.XReadGroupArgs{Group: "test-group", Streams: []string{"test-stream", ">"},
		Consumer: "test-consumer"})
	assert.Len(t, streams, 1)
	assert.Equal(t, "test-stream", streams[0].Stream)
	assert.Len(t, streams[0].Messages, 5)
	assert.Equal(t, "a1", streams[0].Messages[0].Values["name"])
	assert.Equal(t, int64(5), r.XLen("test-stream"))
	infoGroups = r.XInfoGroups("test-stream")
	assert.Len(t, infoGroups, 1)
	assert.Equal(t, int64(1), infoGroups[0].Consumers)
	assert.Equal(t, int64(5), infoGroups[0].Pending)
	streams2 := r.XReadGroup(&redis.XReadGroupArgs{Group: "test-group", Streams: []string{"test-stream", ">"},
		Consumer: "test-consumer", Block: -1})
	assert.Nil(t, streams2)
	streams2 = r.XReadGroup(&redis.XReadGroupArgs{Group: "test-group", Streams: []string{"test-stream", "0"},
		Consumer: "test-consumer", Block: -1})
	assert.Len(t, streams2, 1)
	assert.Len(t, streams2[0].Messages, 5)
	pending := r.XPending("test-stream", "test-group")
	assert.Equal(t, int64(5), pending.Count)
	assert.Equal(t, int64(5), pending.Consumers["test-consumer"])
	pendingExt := r.XPendingExt(&redis.XPendingExtArgs{Stream: "test-stream", Group: "test-group", Count: 10, Start: "-", End: "+"})
	assert.Len(t, pendingExt, 5)
	assert.Equal(t, "test-consumer", pendingExt[0].Consumer)
	assert.Equal(t, int64(2), pendingExt[0].RetryCount)
	time.Sleep(time.Millisecond * 2)
	messages := r.XClaim(&redis.XClaimArgs{Stream: "test-stream", Group: "test-group", Consumer: "test-consumer-2",
		MinIdle:  time.Millisecond,
		Messages: []string{pendingExt[0].ID, pendingExt[1].ID}})
	assert.Len(t, messages, 2)
	pendingExt = r.XPendingExt(&redis.XPendingExtArgs{Stream: "test-stream", Group: "test-group", Count: 10, Start: "-",
		End: "+", Consumer: "test-consumer"})
	assert.Len(t, pendingExt, 3)
	testID := pendingExt[0].ID
	pendingExt = r.XPendingExt(&redis.XPendingExtArgs{Stream: "test-stream", Group: "test-group", Count: 10, Start: "-",
		End: "+", Consumer: "test-consumer-2"})
	assert.Len(t, pendingExt, 2)
	infoGroups = r.XInfoGroups("test-stream")
	assert.Len(t, infoGroups, 1)
	assert.Equal(t, int64(2), infoGroups[0].Consumers)
	assert.Equal(t, int64(5), infoGroups[0].Pending)
	confirmed := r.XAck("test-stream", "test-group", pendingExt[0].ID, pendingExt[1].ID)
	assert.Equal(t, int64(2), confirmed)
	pendingExt = r.XPendingExt(&redis.XPendingExtArgs{Stream: "test-stream", Group: "test-group", Count: 10, Start: "-",
		End: "+", Consumer: "test-consumer-2"})
	assert.Len(t, pendingExt, 0)
	infoGroups = r.XInfoGroups("test-stream")
	assert.Len(t, infoGroups, 1)
	assert.Equal(t, int64(2), infoGroups[0].Consumers)
	assert.Equal(t, int64(3), infoGroups[0].Pending)
	ids := r.XClaimJustID(&redis.XClaimArgs{Stream: "test-stream", Group: "test-group", Consumer: "test-consumer-2",
		MinIdle: time.Millisecond, Messages: []string{testID, "2-2"}})
	assert.Len(t, ids, 1)
	assert.Equal(t, testID, ids[0])
	confirmed = r.XGroupDelConsumer("test-stream", "test-group", "test-consumer-2")
	assert.Equal(t, int64(1), confirmed)
	infoGroups = r.XInfoGroups("test-stream")
	assert.Equal(t, int64(1), infoGroups[0].Consumers)

	engine.GetEventBroker().PublishMap("test-stream-a", EventAsMap{"name": "a1"})
	engine.GetEventBroker().PublishMap("test-stream-b", EventAsMap{"name": "b1"})
	r.XGroupCreate("test-stream-a", "test-group-ab", "0")
	r.XGroupCreate("test-stream-b", "test-group-ab", "0")
	streams = r.XReadGroup(&redis.XReadGroupArgs{Group: "test-group-ab", Streams: []string{"test-stream-a", "test-stream-b", ">", ">"},
		Consumer: "test-consumer-ab", Block: -1})
	assert.Len(t, streams, 2)
	assert.Len(t, streams[0].Messages, 1)
	assert.Len(t, streams[1].Messages, 1)
}
