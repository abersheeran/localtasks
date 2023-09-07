package localtasks

import (
	"context"
	"net/url"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

type Queue struct {
	redis *redis.Client

	stream_name      string
	delay_queue_name string
	group_name       string

	add_no_delay_task_script  *redis.Script
	add_delay_task_script     *redis.Script
	set_delay_task_script     *redis.Script
	ack_task_script           *redis.Script
	store_latest_error_script *redis.Script
	retry_task_script         *redis.Script
	delete_task_script        *redis.Script
}

func NewQueue(redis_dsn string) *Queue {
	u, err := url.Parse(redis_dsn)
	if err != nil {
		panic(err)
	}

	password, _ := u.User.Password()

	var db int
	if u.Path != "" {
		db, _ = strconv.Atoi(u.Path)
	} else {
		db = 0
	}

	rdb := redis.NewClient(&redis.Options{
		Addr:     u.Host,
		Password: password,
		DB:       db,
	})

	return &Queue{
		redis: rdb,

		stream_name:      "localtasks",
		delay_queue_name: "localtasks:delay",
		group_name:       "workers",

		add_no_delay_task_script: redis.NewScript(
			`
			local stream_name = KEYS[1]
			local task_id = ARGV[1]
			local task_json = ARGV[2]

			local res = redis.call('HSETNX', task_id, 'json', task_json)
			if res == 0 then
				return 0
			end

			local message_id = redis.call('XADD', stream_name, '*', 'json', task_json)
			redis.call('HSET', task_id, 'message_id', message_id)

			return 1
			`,
		),
		add_delay_task_script: redis.NewScript(
			`
			local delay_queue_name = KEYS[1]
			local key = ARGV[1]
			local milliseconds = tonumber(ARGV[2])
			local task_json = ARGV[3]

			local res = redis.call('HSETNX', key, 'json', task_json)
			if res == 0 then
				return 0
			end

			return redis.call('ZADD', delay_queue_name, 'NX', milliseconds, key)
			`,
		),
		set_delay_task_script: redis.NewScript(
			`
			local delay_queue_name = KEYS[1]
			local stream_name = KEYS[2]
			local now_milliseconds = tonumber(ARGV[1])

			local member = redis.call('ZPOPMIN', delay_queue_name, 1)
			local task_id = member[1]
			local milliseconds = tonumber(member[2])

			if task_id == nil then
				return nil
			end

			if milliseconds > now_milliseconds then
				redis.call('ZADD', delay_queue_name, milliseconds, task_id)
				return nil
			end

			local task_json = redis.call('HGET', task_id, 'json')
			-- https://redis.io/commands/hget/
			-- What's the fuck? boolean???
			if (type(task_json) == "boolean" and not task_json) or task_json == nil then
				return nil
			end

			local message_id = redis.call('XADD', stream_name, '*', 'json', task_json)
			if message_id == nil then
				return nil
			end

			redis.call('HSET', task_id, 'message_id', message_id)

			return true
			`,
		),
		ack_task_script: redis.NewScript(
			`
			local stream_name = KEYS[1]
			local group_name = KEYS[2]
			local message_id = ARGV[1]
			local task_id = ARGV[2]

			redis.call('XACK', stream_name, group_name, message_id)
			redis.call('XDEL', stream_name, message_id)
			redis.call('DEL', task_id)
			`,
		),
		store_latest_error_script: redis.NewScript(
			`
			local task_id = KEYS[1]
			local error = ARGV[1]

			redis.call('HSET', task_id, 'latest_error', error)
			local error_count = redis.call('HINCRBY', task_id, 'error_count', 1)
			return error_count
			`,
		),
		retry_task_script: redis.NewScript(
			`
			local stream_name = KEYS[1]
			local group_name = KEYS[2]
			local delay_queue_name = KEYS[3]
			local message_id = ARGV[1]
			local task_id = ARGV[2]
			local milliseconds = tonumber(ARGV[3])

			redis.call('XACK', stream_name, group_name, message_id)
			redis.call('XDEL', stream_name, message_id)
			local res = redis.call('HDEL', task_id, 'message_id')
			if res == 0 then  -- task_id not exists
				return nil
			end

			redis.call('ZADD', delay_queue_name, 'NX', milliseconds, task_id)
			`,
		),
		delete_task_script: redis.NewScript(
			`
			local stream_name = KEYS[1]
			local delay_queue_name = KEYS[2]
			local task_id = ARGV[1]

			local message_id = redis.call('HGET', task_id, "message_id")
			if message_id then
				redis.call('XDEL', stream_name, message_id)
			else
				redis.call('ZREM', delay_queue_name, task_id)
			end
			redis.call('DEL', task_id)
			`,
		),
	}
}

func (queue *Queue) Push(ctx context.Context, task *Task, delay_milliseconds int64) (bool, error) {
	if delay_milliseconds == 0 {
		keys := []string{queue.stream_name}
		args := []interface{}{task.ID, task.Json()}
		return queue.add_no_delay_task_script.Run(ctx, queue.redis, keys, args).Bool()
	} else {
		milliseconds := time.Now().UnixMilli() + delay_milliseconds
		keys := []string{queue.delay_queue_name}
		args := []interface{}{task.ID, milliseconds, task.Json()}
		return queue.add_delay_task_script.Run(ctx, queue.redis, keys, args).Bool()
	}
}

func (queue *Queue) SetDelayTask(ctx context.Context) (bool, error) {
	keys := []string{queue.delay_queue_name, queue.stream_name}
	args := []interface{}{time.Now().UnixMilli()}
	return queue.set_delay_task_script.Run(ctx, queue.redis, keys, args).Bool()
}

type PullResult struct {
	MessageId string
	Task      *Task
}

func (queue *Queue) Pull(ctx context.Context, consumer string) PullResult {
	queue.SetDelayTask(ctx)

	res := queue.redis.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group:    queue.group_name,
		Consumer: consumer,
		Streams:  []string{queue.stream_name, ">"},
		Count:    1,
	}).Val()

	if len(res) == 0 {
		return PullResult{}
	}

	for _, xstream := range res {
		for _, message := range xstream.Messages {
			task_json := message.Values["json"].(string)
			return PullResult{
				MessageId: message.ID,
				Task:      (&Task{}).FromJson(task_json),
			}
		}
	}

	return PullResult{}
}

func (queue *Queue) Ack(ctx context.Context, message_id string, task_id string) {
	keys := []string{queue.stream_name, queue.group_name}
	args := []interface{}{message_id, task_id}
	queue.ack_task_script.Run(ctx, queue.redis, keys, args).Result()
}

func (queue *Queue) StoreLatestError(ctx context.Context, task_id string, err string) int64 {
	keys := []string{task_id}
	args := []interface{}{err}
	res, _ := queue.store_latest_error_script.Run(ctx, queue.redis, keys, args).Int64()
	return res
}

func (queue *Queue) Retry(ctx context.Context, message_id string, task_id string, delay_milliseconds int64) {
	keys := []string{queue.stream_name, queue.group_name, queue.delay_queue_name}
	args := []interface{}{message_id, task_id, time.Now().UnixMilli() + delay_milliseconds}
	queue.retry_task_script.Run(ctx, queue.redis, keys, args).Result()
}

func (queue *Queue) Delete(ctx context.Context, task_id string) {
	keys := []string{queue.stream_name, queue.delay_queue_name}
	args := []interface{}{task_id}
	queue.delete_task_script.Run(ctx, queue.redis, keys, args).Result()
}

func (queue *Queue) Autoclaim(ctx context.Context, consumer string, min_idle_time int64) []PullResult {
	messages, _ := queue.redis.XAutoClaim(ctx, &redis.XAutoClaimArgs{
		Stream:   queue.stream_name,
		Group:    queue.group_name,
		Consumer: consumer,
		MinIdle:  time.Duration(min_idle_time),
	}).Val()

	results := []PullResult{}
	for _, message := range messages {
		task_json := message.Values["json"].(string)
		task := (&Task{}).FromJson(task_json)
		results = append(results, PullResult{
			MessageId: message.ID,
			Task:      task,
		})
	}
	return results
}

func (queue *Queue) Pending(ctx context.Context) struct {
	Count     int64
	Lower     string
	Higher    string
	Consumers map[string]int64
} {
	pending_info, err := queue.redis.XPending(ctx, queue.stream_name, queue.group_name).Result()
	if err != nil {
		panic(err)
	}
	return *pending_info
}
