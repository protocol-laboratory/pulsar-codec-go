// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package pnet

import (
	"fmt"
	"github.com/protocol-laboratory/pulsar-codec-go/codec"
	"github.com/protocol-laboratory/pulsar-codec-go/pb"
	"net"
	"sync"
)

type PulsarNetClientConfig struct {
	Host             string
	Port             int
	BufferMax        int
	SendQueueSize    int
	PendingQueueSize int
}

func (p PulsarNetClientConfig) addr() string {
	return fmt.Sprintf("%s:%d", p.Host, p.Port)
}

type sendRequest struct {
	bytes    []byte
	callback func([]byte, error)
}

type PulsarNetClient struct {
	conn         net.Conn
	eventsChan   chan *sendRequest
	pendingQueue chan *sendRequest
	buffer       *buffer
	closeCh      chan struct{}
}

func (p *PulsarNetClient) CommandConnect(commandConnect *pb.CommandConnect) (*pb.CommandConnected, error) {
	baseCommand, err := p.req(&pb.BaseCommand{
		Type:    pb.BaseCommand_CONNECT.Enum(),
		Connect: commandConnect,
	})
	if err != nil {
		return nil, err
	}
	return baseCommand.Connected, nil
}

func (p *PulsarNetClient) req(request *pb.BaseCommand) (*pb.BaseCommand, error) {
	reqBytes, err := request.Marshal()
	if err != nil {
		return nil, err
	}
	finalBytes := make([]byte, len(reqBytes)+8)
	codec.PutInt(finalBytes, 0, len(reqBytes)+4)
	codec.PutInt(finalBytes, 4, len(reqBytes))
	copy(finalBytes[8:], reqBytes)
	bytes, err := p.Send(finalBytes)
	if err != nil {
		return nil, err
	}
	resp := &pb.BaseCommand{}
	err = resp.Unmarshal(bytes[4:])
	if err != nil {
		return nil, err
	}
	return resp, nil
}

func (p *PulsarNetClient) Send(bytes []byte) ([]byte, error) {
	wg := sync.WaitGroup{}
	wg.Add(1)
	var result []byte
	var err error
	p.sendAsync(bytes, func(resp []byte, e error) {
		result = resp
		err = e
		wg.Done()
	})
	wg.Wait()
	if err != nil {
		return nil, err
	}
	return result[4:], nil
}

func (p *PulsarNetClient) sendAsync(bytes []byte, callback func([]byte, error)) {
	sr := &sendRequest{
		bytes:    bytes,
		callback: callback,
	}
	p.eventsChan <- sr
}

func (p *PulsarNetClient) read() {
	for {
		select {
		case req := <-p.pendingQueue:
			n, err := p.conn.Read(p.buffer.bytes[p.buffer.cursor:])
			if err != nil {
				req.callback(nil, err)
				p.closeCh <- struct{}{}
				break
			}
			p.buffer.cursor += n
			if p.buffer.cursor < 4 {
				continue
			}
			length := int(p.buffer.bytes[3]) | int(p.buffer.bytes[2])<<8 | int(p.buffer.bytes[1])<<16 | int(p.buffer.bytes[0])<<24 + 4
			if p.buffer.cursor < length {
				continue
			}
			if length > p.buffer.max {
				req.callback(nil, fmt.Errorf("response length %d is too large", length))
				p.closeCh <- struct{}{}
				break
			}
			req.callback(p.buffer.bytes[:length], nil)
			p.buffer.cursor -= length
			copy(p.buffer.bytes[:p.buffer.cursor], p.buffer.bytes[length:])
		case <-p.closeCh:
			return
		}
	}
}

func (p *PulsarNetClient) write() {
	for {
		select {
		case req := <-p.eventsChan:
			n, err := p.conn.Write(req.bytes)
			if err != nil {
				req.callback(nil, err)
				p.closeCh <- struct{}{}
				break
			}
			if n != len(req.bytes) {
				req.callback(nil, fmt.Errorf("write %d bytes, but expect %d bytes", n, len(req.bytes)))
				p.closeCh <- struct{}{}
				break
			}
			p.pendingQueue <- req
		case <-p.closeCh:
			return
		}
	}
}

func (p *PulsarNetClient) Close() {
	_ = p.conn.Close()
	p.closeCh <- struct{}{}
}

func NewPulsarNetClient(config PulsarNetClientConfig) (*PulsarNetClient, error) {
	conn, err := net.Dial("tcp", config.addr())
	if err != nil {
		return nil, err
	}
	if config.SendQueueSize == 0 {
		config.SendQueueSize = 1000
	}
	if config.PendingQueueSize == 0 {
		config.PendingQueueSize = 1000
	}
	if config.BufferMax == 0 {
		config.BufferMax = 512 * 1024
	}
	p := &PulsarNetClient{}
	p.conn = conn
	p.eventsChan = make(chan *sendRequest, config.SendQueueSize)
	p.pendingQueue = make(chan *sendRequest, config.PendingQueueSize)
	p.buffer = &buffer{
		max:    config.BufferMax,
		bytes:  make([]byte, config.BufferMax),
		cursor: 0,
	}
	p.closeCh = make(chan struct{})
	go func() {
		p.read()
	}()
	go func() {
		p.write()
	}()
	return p, nil
}
