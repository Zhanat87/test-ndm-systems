package main

import (
	"context"
	"net/http"
	"os"
	"strconv"
	"sync"
	"time"
)

type queue struct {
	messages []string
	waiters  []chan string
}

type broker struct {
	mu     sync.Mutex
	queues map[string]*queue
}

func newBroker() *broker {
	return &broker{
		queues: make(map[string]*queue),
	}
}

func (b *broker) getQueue(name string) *queue {
	q, ok := b.queues[name]
	if !ok {
		q = &queue{}
		b.queues[name] = q
	}
	return q
}

func (b *broker) put(name, msg string) {
	b.mu.Lock()
	defer b.mu.Unlock()

	q := b.getQueue(name)

	// если есть ожидающие — отдаем сразу
	if len(q.waiters) > 0 {
		ch := q.waiters[0]
		q.waiters = q.waiters[1:]
		ch <- msg
		return
	}

	q.messages = append(q.messages, msg)
}

func (b *broker) get(name string, timeout int) (string, bool) {
	b.mu.Lock()
	q := b.getQueue(name)

	// есть сообщения
	if len(q.messages) > 0 {
		msg := q.messages[0]
		q.messages = q.messages[1:]
		b.mu.Unlock()
		return msg, true
	}

	// нет timeout
	if timeout <= 0 {
		b.mu.Unlock()
		return "", false
	}

	// создаем waiter
	ch := make(chan string, 1)
	q.waiters = append(q.waiters, ch)
	b.mu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(timeout)*time.Second)
	defer cancel()

	select {
	case msg := <-ch:
		return msg, true
	case <-ctx.Done():
		// удалить waiter
		b.mu.Lock()
		q := b.getQueue(name)
		for i, w := range q.waiters {
			if w == ch {
				q.waiters = append(q.waiters[:i], q.waiters[i+1:]...)
				break
			}
		}
		b.mu.Unlock()
		return "", false
	}
}

func main() {
	port := "8080"
	if len(os.Args) > 1 {
		port = os.Args[1]
	}

	b := newBroker()

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		queueName := r.URL.Path[1:]
		if queueName == "" {
			w.WriteHeader(http.StatusNotFound)
			return
		}

		switch r.Method {

		case http.MethodPut:
			msg := r.URL.Query().Get("v")
			if msg == "" {
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			b.put(queueName, msg)
			w.WriteHeader(http.StatusOK)

		case http.MethodGet:
			timeoutStr := r.URL.Query().Get("timeout")
			timeout := 0
			if timeoutStr != "" {
				t, err := strconv.Atoi(timeoutStr)
				if err == nil && t > 0 {
					timeout = t
				}
			}

			msg, ok := b.get(queueName, timeout)
			if !ok {
				w.WriteHeader(http.StatusNotFound)
				return
			}

			w.Write([]byte(msg))

		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	})

	http.ListenAndServe(":"+port, nil)
}
