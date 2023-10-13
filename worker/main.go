package main

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"net/http"
	"strings"
	"time"

	. "abersheeran/localtasks"

	log "github.com/sirupsen/logrus"
)

func fetch(ctx context.Context, queue *Queue, client *http.Client, message_id string, task *Task) {
	settings := NewSettings()
	task_info := strings.Join([]string{task.ID, task.Method, task.Url}, " ")
	log.Info("Running task: " + task_info)
	request, err := http.NewRequest(task.Method, task.Url, bytes.NewReader(task.Payload))
	if err != nil {
		log.Error("Failed to create request for task: " + task_info)
		queue.StoreLatestError(ctx, message_id, err.Error()).Unwrap()
		return
	}
	for key, value := range task.Headers {
		request.Header.Set(key, value)
	}
	start_time := time.Now()
	response, err := client.Do(request)
	elapsed_total_second := time.Since(start_time) / time.Second
	err_message := ""
	if err == nil {
		defer response.Body.Close()
		if response.StatusCode >= 200 && response.StatusCode < 300 {
			queue.Ack(ctx, message_id, task.ID).Unwrap()
			log.Info(
				fmt.Sprint("Task ", task_info, " ",
					response.StatusCode, " cast ", elapsed_total_second, "s"),
			)
			return
		} else {
			err_message = response.Status
		}
	} else {
		err_message = err.Error()
	}
	log.Error("Failed to run task: " + task_info + " " + err_message)
	err_count := queue.StoreLatestError(ctx, message_id, err_message).Unwrap()
	if settings.Retry.MaxRetries <= err_count {
		log.Warning("Task " + task_info + " max retries reached")
		queue.Ack(ctx, message_id, task.ID).Unwrap()
		return
	}

	var retry_times int
	if err_count > settings.Retry.MaxDoubling {
		retry_times = settings.Retry.MaxDoubling
	} else {
		retry_times = err_count
	}

	delay_seconds := math.Min(
		float64(settings.Retry.MinInterval)*(math.Pow(2, float64(retry_times))),
		float64(settings.Retry.MaxInterval),
	)
	queue.Retry(ctx, message_id, task.ID, int64(delay_seconds*1000)).Unwrap()
}

func main() {
	settings := NewSettings()
	queue := NewQueue(settings.RedisDsn)
	client := &http.Client{}

	ctx := context.TODO()

	for {
		for _, message := range queue.Autoclaim(ctx, settings.ConsumerName, int64((settings.Retry.Timeout)+60)*1000) {
			go fetch(ctx, queue, client, message.MessageId, message.Task)
		}

		pending := queue.Pending(ctx).Unwrap()
		if pending.Count >= int64(settings.SpeedLimit.MaxConcurrent) {
			time.Sleep(200 * time.Millisecond)
		}

		message := queue.Pull(ctx, settings.ConsumerName)
		if message == nil {
			time.Sleep(200 * time.Millisecond)
			continue
		}

		go fetch(ctx, queue, client, message.MessageId, message.Task)
	}
}
