package sse

import (
	"bufio"
	"fmt"
	"io"
	"net/http"
)

// Prepare sets Server-Sent-Events response headers and returns the flusher.
func Prepare(w http.ResponseWriter) (http.Flusher, error) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		return nil, fmt.Errorf("streaming unsupported")
	}
	h := w.Header()
	h.Set("Content-Type", "text/event-stream")
	h.Set("Cache-Control", "no-cache")
	h.Set("Connection", "keep-alive")
	h.Set("X-Accel-Buffering", "no") // tell nginx not to buffer the stream
	w.WriteHeader(http.StatusOK)
	flusher.Flush()
	return flusher, nil
}

// WriteEvent writes one SSE event (event name optional) and flushes.
func WriteEvent(w io.Writer, flusher http.Flusher, event, data string) error {
	if event != "" {
		if _, err := fmt.Fprintf(w, "event: %s\n", event); err != nil {
			return err
		}
	}
	if _, err := fmt.Fprintf(w, "data: %s\n\n", data); err != nil {
		return err
	}
	flusher.Flush()
	return nil
}

// Relay copies an upstream SSE stream to the client line-by-line, flushing as it
// goes. Used by the gateway to proxy the processing-backend stream.
func Relay(w io.Writer, flusher http.Flusher, upstream io.Reader) error {
	reader := bufio.NewReader(upstream)
	for {
		line, err := reader.ReadBytes('\n')
		if len(line) > 0 {
			if _, werr := w.Write(line); werr != nil {
				return werr
			}
			flusher.Flush()
		}
		if err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}
	}
}
