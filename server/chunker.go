package server

import (
	"io"
)

// Chunk - returns chunks from the stream splitter.
type Chunk struct {
	Index int
	Data  []byte
	Err   error
}

// Chunker reads from io.Reader, splits the data into chunks, and sends
// each chunk to the channel. This method runs until an EOF or error occurs. If
// an error occurs, the method sends the error over the channel and returns.
// Before returning, the channel is always closed.
//
// The user should run this as a gorountine and retrieve the data over the
// channel.
//
//  for chunk := range Chunker(reader, chunkSize) {
//    log.Println(chunk.Data)
//  }
func Chunker(reader io.Reader, chunkSize uint64) <-chan Chunk {
	ch := make(chan Chunk)
	go chunkIt(reader, chunkSize, ch)
	return ch
}

func chunkIt(reader io.Reader, chunkSize uint64, ch chan Chunk) {
	buf := make([]byte, chunkSize)
	var cindex int
	for {
		n, err := io.ReadFull(reader, buf)
		if n > 0 {
			ch <- Chunk{Index: cindex, Data: buf[:n]}
			cindex++
			continue
		}
		ch <- Chunk{Err: err}
		break
	}

	// close the channel, signaling the channel reader that the stream is complete
	close(ch)
}
