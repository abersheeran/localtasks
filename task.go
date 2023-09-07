package localtasks

import "encoding/json"

type Task struct {
	ID      string
	Url     string
	Method  string
	Headers map[string]string
	Payload []byte
}

func (task *Task) Json() string {
	task_json_bytes, err := json.Marshal(task)
	if err != nil {
		panic(err)
	}
	task_json := string(task_json_bytes)
	return task_json
}

func (task *Task) FromJson(task_json string) *Task {
	err := json.Unmarshal([]byte(task_json), task)
	if err != nil {
		panic(err)
	}
	return task
}
