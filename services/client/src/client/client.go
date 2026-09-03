package client

import (
	"os"
	"bufio"
	"net"
	"time"
	"encoding/binary"
	"strings"
	"fmt"

	"github.com/7574-sistemas-distribuidos/tp-nivelador/src/logger"
	"github.com/7574-sistemas-distribuidos/tp-nivelador/src/safe_socket"
)

const CONNECTION_ATTEMPTS_MAX = 3
const CONNECTION_ATTEMPS_DELAY_MS = 200


type ClientConfig struct {
	ServerHost string
	ServerPort string
	AgencyId   string
	InputFile  string
	OutputFile string
	BatchSize  int
}

type Client struct {
	conn   net.Conn
	config ClientConfig
}

func NewClient(config ClientConfig) (*Client, error) {
	conn, err := connectToServer(config.ServerHost, config.ServerPort)
	if err != nil {
		logger.Warn("connect-to-server", logger.Fail)
		return nil, err
	}

	client := &Client{conn: conn, config: config}
	return client, nil
}

func connectToServer(host, port string) (net.Conn, error) {
	const action = "connect-to-server"
	var err error
	var conn net.Conn

	logger.Info(action, logger.InProgress)
	for i := range CONNECTION_ATTEMPTS_MAX {
		conn, err = net.Dial("tcp", host+":"+port)
		if err != nil {
			logger.Warn(action, logger.Fail, "attempt", i)
			time.Sleep(CONNECTION_ATTEMPS_DELAY_MS * time.Millisecond)
			continue
		}

		logger.Info(action, logger.Success)
		break
	}

	return conn, err
}

func (client *Client) sendBatch(batch []string) error {
	if len(batch) == 0 {
		return nil
	}

	message := strings.Join(batch, "\n")
	mesageBytes := []byte(message)
	messageSize := len(mesageBytes)

	sizeBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(sizeBytes, uint32(messageSize))

	if err := safe_socket.SendAll(client.conn, sizeBytes); err != nil {
		logger.Error("send-batch-size", logger.Fail, "error", err)
		return err
	}

	if err := safe_socket.SendAll(client.conn, mesageBytes); err != nil {
		logger.Error("send-batch", logger.Fail, "error", err)
		return err
	}

	return nil
}

func (client *Client) Run() error {
	const mainAction = "send-bets"
	defer client.conn.Close()

	// opens file for INPUT
	input, err := os.Open(client.config.InputFile)
	if err != nil {
		logger.Error("open-input-file", logger.Fail, "file", client.config.InputFile)
		return err
	}
	defer input.Close()

	// opens file for OUTPUT
	output, err := os.OpenFile(
		client.config.OutputFile,
		os.O_APPEND|os.O_CREATE|os.O_WRONLY,
		0644,
	)
	if err != nil {
		logger.Error("open-output-file", logger.Fail, "file", client.config.OutputFile)
		return err
	}
	defer output.Close()
	
	scanner := bufio.NewScanner(input)

	batch := []string{}
	
	for scanner.Scan() {
		line := scanner.Text()

		messageArgs := []any{"agency-id", client.config.AgencyId, "line", line}
		logger.Info(mainAction, logger.InProgress, messageArgs...)

		lineMessage := client.config.AgencyId + "," + line
		
		if len(batch) < client.config.BatchSize {
        batch = append(batch, lineMessage)
        continue
    }
 
    if err := client.sendBatch(batch); err != nil {
        return err
    }
        
    batch = []string{lineMessage}

	}

	if err := scanner.Err(); err != nil {
		logger.Error("scan-input-file", logger.Fail, "file", client.config.InputFile)
		return err
	}

	// Send remaining batch
	if err := client.sendBatch(batch); err != nil {
		logger.Error("send-batch", logger.Fail, "error", err)
		return err
	}
	
	// Client ends communication by sending 4 zero bytes
	endMarker := make([]byte, 4)
	if err := safe_socket.SendAll(client.conn, endMarker); err != nil {
		logger.Error("send-end-marker", logger.Fail, "agency-id", client.config.AgencyId)
		return err
	}

	responseSizeBytes, err := safe_socket.RecvAll(client.conn, 4)
	if err != nil {
		logger.Error("receive-response-size", logger.Fail, "agency-id", client.config.AgencyId)
		return err
	}
	
	// Gets a response from the server
	responseSize := binary.BigEndian.Uint32(responseSizeBytes)
	response, err := safe_socket.RecvAll(client.conn, int(responseSize))
	if err != nil {
		logger.Error("receive-response", logger.Fail, "agency-id", client.config.AgencyId)
		return err
	}
	
	responseParts := strings.SplitN(string(response), "\n", 2)
	if responseParts[0] != "OK" {
		logger.Error("server-response-status", logger.Fail, "agency-id", client.config.AgencyId, "response", string(response))
		return fmt.Errorf("server rejected bets: %s", response)
	}

	winners := []byte{}
	if len(responseParts) > 1 {
		winners = []byte(responseParts[1])
	}

	_, err = output.Write(winners)
	if err != nil {
		logger.Error("write-response", logger.Fail, "agency-id", client.config.AgencyId)
		return err
	}

	logger.Info(mainAction, logger.Success, "agency-id", client.config.AgencyId)

	return nil
}
