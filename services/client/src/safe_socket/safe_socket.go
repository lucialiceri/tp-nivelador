package safe_socket

import "io"

//TODO: Complete with a short-read/short-write tolerant implementation

func SendAll(socket io.Writer, bytes []byte) error {
	n, err := socket.Write(bytes)
	if err != nil {
		return err
	}
	for n < len(bytes) {
		remaining, err := socket.Write(bytes[n:])
		if err != nil {
			return err
		}
		n += remaining
	}
	return n
}

func RecvAll(socket io.Reader, size int) ([]byte, error) {
	buff := make([]byte, size)
	n, err := socket.Read(buff)
	if err != nil {
		return nil, err
	}
	for n < size {
		remaining, err := socket.Read(buff[n:])
		if err != nil {
			return nil, err
		}
		n += remaining
	}
	return buff[:n], nil
}
