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

//go:build linux || freebsd || dragonfly || darwin

package pgnet

import (
	"fmt"
	"github.com/panjf2000/gnet"
	"sync"
	"time"
)

type GnetClientConfig struct {
	Host             string
	Port             int
	SendQueueSize    int
	PendingQueueSize int
}

func (g GnetClientConfig) addr() string {
	return fmt.Sprintf("%s:%d", g.Host, g.Port)
}

type PulsarGnetClient struct {
	networkClient *gnet.Client
	conn          gnet.Conn
	eventsChan    chan *sendRequest
	pendingQueue  chan *sendRequest
	closeCh       chan struct{}
}

type sendRequest struct {
	bytes    []byte
	callback func([]byte, error)
}

func (p *PulsarGnetClient) run() {
	for {
		select {
		case req := <-p.eventsChan:
			err := p.conn.AsyncWrite(req.bytes)
			if err != nil {
				req.callback(nil, err)
				break
			}
			p.pendingQueue <- req
		case <-p.closeCh:
			return
		}
	}
}

func (p *PulsarGnetClient) Send(bytes []byte) ([]byte, error) {
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
	return result, err
}

func (p *PulsarGnetClient) sendAsync(bytes []byte, callback func([]byte, error)) {
	sr := &sendRequest{
		bytes:    bytes,
		callback: callback,
	}
	p.eventsChan <- sr
}

func (p *PulsarGnetClient) OnInitComplete(server gnet.Server) (action gnet.Action) {
	return gnet.None
}

func (p *PulsarGnetClient) OnShutdown(server gnet.Server) {
}

func (p *PulsarGnetClient) OnOpened(c gnet.Conn) (out []byte, action gnet.Action) {
	return
}

func (p *PulsarGnetClient) OnClosed(c gnet.Conn, err error) (action gnet.Action) {
	return
}

func (p *PulsarGnetClient) PreWrite(c gnet.Conn) {
}

func (p *PulsarGnetClient) AfterWrite(c gnet.Conn, b []byte) {
}

func (p *PulsarGnetClient) React(packet []byte, c gnet.Conn) (out []byte, action gnet.Action) {
	request := <-p.pendingQueue
	request.callback(packet, nil)
	return nil, gnet.None
}

func (p *PulsarGnetClient) Tick() (delay time.Duration, action gnet.Action) {
	return 15 * time.Second, gnet.None
}

func (p *PulsarGnetClient) Close() {
	_ = p.networkClient.Stop()
	p.closeCh <- struct{}{}
}

func NewPulsarGnetClient(config GnetClientConfig) (*PulsarGnetClient, error) {
	p := &PulsarGnetClient{}
	var err error
	p.networkClient, err = gnet.NewClient(p, gnet.WithCodec(pulsarCodec))
	if err != nil {
		return nil, err
	}
	err = p.networkClient.Start()
	if err != nil {
		return nil, err
	}
	p.conn, err = p.networkClient.Dial("tcp", config.addr())
	if err != nil {
		return nil, err
	}
	if config.SendQueueSize == 0 {
		config.SendQueueSize = 1000
	}
	if config.PendingQueueSize == 0 {
		config.PendingQueueSize = 1000
	}
	p.eventsChan = make(chan *sendRequest, config.SendQueueSize)
	p.pendingQueue = make(chan *sendRequest, config.PendingQueueSize)
	p.closeCh = make(chan struct{})
	go func() {
		p.run()
	}()
	return p, nil
}
