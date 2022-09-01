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
	"github.com/gogo/protobuf/proto"
	"github.com/protocol-laboratory/pulsar-codec-go/pb"
	"net"
)

type PulsarNetServerConfig struct {
	Host      string
	Port      int
	BufferMax int
}

type PulsarNetServerImpl interface {
	AcceptError(conn net.Conn, err error)
	ReadError(conn net.Conn, err error)
	ReactError(conn net.Conn, err error)
	WriteError(conn net.Conn, err error)

	CommandConnect(connect *pb.CommandConnect) (*pb.CommandConnected, error)
}

func (p *PulsarNetServerConfig) addr() string {
	return fmt.Sprintf("%s:%d", p.Host, p.Port)
}

type PulsarNetServer struct {
	listener net.Listener
	impl     PulsarNetServerImpl
	config   PulsarNetServerConfig
}

func (p *PulsarNetServer) run() {
	for {
		conn, err := p.listener.Accept()
		if err != nil {
			p.impl.AcceptError(conn, err)
			break
		}
		go p.handleConn(&pulsarConn{
			conn: conn,
			buffer: &buffer{
				max:    p.config.BufferMax,
				bytes:  make([]byte, p.config.BufferMax),
				cursor: 0,
			},
		})
	}
}

type pulsarConn struct {
	conn   net.Conn
	buffer *buffer
}

func (p *PulsarNetServer) handleConn(pulsarConn *pulsarConn) {
	for {
		readLen, err := pulsarConn.conn.Read(pulsarConn.buffer.bytes[pulsarConn.buffer.cursor:])
		if err != nil {
			p.impl.ReadError(pulsarConn.conn, err)
			break
		}
		pulsarConn.buffer.cursor += readLen
		if pulsarConn.buffer.cursor < 4 {
			continue
		}
		length := int(pulsarConn.buffer.bytes[3]) | int(pulsarConn.buffer.bytes[2])<<8 | int(pulsarConn.buffer.bytes[1])<<16 | int(pulsarConn.buffer.bytes[0])<<24 + 4
		if pulsarConn.buffer.cursor < length {
			continue
		}
		if length > pulsarConn.buffer.max {
			p.impl.ReadError(pulsarConn.conn, fmt.Errorf("message too long: %d", length))
			break
		}
		dstBytes, err := p.react(pulsarConn.buffer.bytes[:length])
		if err != nil {
			p.impl.ReactError(pulsarConn.conn, err)
			break
		}
		write, err := pulsarConn.conn.Write(dstBytes)
		if err != nil {
			p.impl.WriteError(pulsarConn.conn, err)
			break
		}
		if write != len(dstBytes) {
			p.impl.WriteError(pulsarConn.conn, fmt.Errorf("write %d bytes, but expect %d bytes", write, len(dstBytes)))
			break
		}
		pulsarConn.buffer.cursor -= length
		copy(pulsarConn.buffer.bytes[:pulsarConn.buffer.cursor], pulsarConn.buffer.bytes[length:])
	}
}

func (p *PulsarNetServer) react(bytes []byte) ([]byte, error) {
	req := &pb.BaseCommand{}
	err := proto.Unmarshal(bytes[8:], req)
	if err != nil {
		return nil, err
	}
	var baseCommand *pb.BaseCommand
	switch req.GetType() {
	case pb.BaseCommand_CONNECT:
		commandConnected, err := p.impl.CommandConnect(req.Connect)
		if err != nil {
			return nil, err
		}
		baseCommand = &pb.BaseCommand{
			Type:      pb.BaseCommand_CONNECTED.Enum(),
			Connected: commandConnected,
		}
	}
	marshal, err := pb.MarshalBaseCmd(baseCommand, true)
	if err != nil {
		return nil, err
	}
	return marshal, err
}

func NewPulsarNetServer(config PulsarNetServerConfig, impl PulsarNetServerImpl) (*PulsarNetServer, error) {
	listener, err := net.Listen("tcp", config.addr())
	if err != nil {
		return nil, err
	}
	p := &PulsarNetServer{}
	p.config = config
	if p.config.BufferMax == 0 {
		p.config.BufferMax = 5 * 1024 * 1024
	}
	p.listener = listener
	p.impl = impl
	go func() {
		p.run()
	}()
	return p, nil
}
