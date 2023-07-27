package main

import (
	"context"
	"encoding/json"
	"flag"
	"github.com/guoyk93/rg"
	"github.com/redis/go-redis/v9"
	"io"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"
)

type Record struct {
	Timestamp     time.Time `json:"timestamp"`
	Target        string    `json:"target"`
	DurationMicro int64     `json:"duration_micro"`
	Error         bool      `json:"error"`
	Message       string    `json:"message"`
}

func createRecord(c *redis.Client) (r Record) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
	defer cancel()

	now := time.Now()
	err := c.Ping(ctx).Err()
	dur := time.Since(now).Microseconds()

	r = Record{
		Timestamp:     now,
		DurationMicro: dur,
	}

	if err != nil {
		r.Error = true
		r.Message = err.Error()
	}

	return
}

func writeRecord(w io.Writer, r Record) (err error) {
	var buf []byte
	if buf, err = json.Marshal(r); err != nil {
		return
	}
	buf = append(buf, '\r', '\n')
	_, err = w.Write(buf)
	return
}

func main() {
	var err error
	defer func() {
		if err == nil {
			return
		}
		log.Println("exited with error:", err.Error())
		os.Exit(1)
	}()

	var (
		optURL       string
		optOutput    string
		optInterval  string
		optReconnect bool
	)
	flag.StringVar(&optURL, "url", "redis://localhost:6379", "redis url")
	flag.StringVar(&optOutput, "output", "output.ndjson", "output file")
	flag.StringVar(&optInterval, "interval", "1s", "interval")
	flag.BoolVar(&optReconnect, "reconnect", false, "reconnect redis everytime")
	flag.Parse()

	interval := rg.Must(time.ParseDuration(optInterval))

	output := rg.Must(os.OpenFile(optOutput, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0640))
	defer output.Close()

	opts := rg.Must(redis.ParseURL(optURL))

	// single connection and fail-fast
	opts.PoolSize = 1
	opts.MaxRetries = -1

	target := opts.Addr

	chSig := make(chan os.Signal, 1)
	signal.Notify(chSig, syscall.SIGINT, syscall.SIGTERM)

	var c *redis.Client

	for {

		if optReconnect || c == nil {

			if c != nil {
				_ = c.Close()
				c = nil
			}

			c = redis.NewClient(opts)
		}

		r := createRecord(c)
		r.Target = target

		rg.Must0(writeRecord(output, r))

		select {
		case <-time.After(interval):
			continue
		case sig := <-chSig:
			log.Println("received signal:", sig.String())
			return
		}
	}

}
