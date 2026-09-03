package safe_socket

import "io"


func SendAll(socket io.Writer, bytes []byte) error {
	totalSent := 0 
	retries := 0
	const retriesLimit = 100

	for totalSent < len(bytes) {
		remaining, err := socket.Write(bytes[totalSent:])
		if err != nil {
			return err
		}
		if remaining == 0 {
			retries++
			if retries > retriesLimit {
				return io.ErrShortWrite
			}
			continue
		}
		totalSent += remaining
	}
	return nil
}

func RecvAll(socket io.Reader, size int) ([]byte, error) {
	buff := make([]byte, size)
	n := 0

	for n < size {
		remaining, err := socket.Read(buff[n:])
		if err != nil {
			if err == io.EOF{
				return buff[:n], io.EOF
			}
			return nil, err
		}
		if remaining == 0 {
			return nil, io.EOF
		}
		n += remaining
	}
	return buff[:n], nil
}
