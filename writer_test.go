package main

import (
	"fmt"
	"testing"
	"time"
)

func TestWriter(t *testing.T) {
	conf := &config{
		ChDSN:           "tcp://127.0.0.1:9001?debug=true&username=user1&password=123456&database=default&read_timeout=10&write_timeout=10&alt_hosts=",
		ChDB:            "default",
		ChTable:         "samples",
		ChBatch:         1,
		ChanSize:        8192,
		CHQuantile:      0.75,
		CHMaxSamples:    8192,
		CHMinPeriod:     10,
		HTTPTimeout:     30000000000,
		HTTPAddr:        ":9201",
		HTTPWritePath:   "/write",
		HTTPMetricsPath: "/metrics",
	}

	requests := make(chan *p2cRequest, conf.ChanSize)

	writer, err := NewP2CWriter(conf, requests)
	if err != nil {
		fmt.Printf("Error creating clickhouse writer: %s\n", err.Error())
		t.Error(err)
	}

	writer.Start()

	p2c := new(p2cRequest)
	p2c.name = "prometheus_remote_storage_read_request_duration_seconds_bucket"
	p2c.ts = time.Unix(time.Now().Unix(), 0)
	p2c.val = 0.85
	p2c.tags = []string{"__name__=prometheus_tsdb_compaction_chunk_size_bytes_bucket", "instance=localhost:9090", "job=prometheus", "le=2767.921875"}
	requests <- p2c
	time.Sleep(5 * time.Minute)
}
