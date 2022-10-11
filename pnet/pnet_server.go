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
	"github.com/protocol-laboratory/pulsar-codec-go/codec"
	"github.com/protocol-laboratory/pulsar-codec-go/pb"
	"net"
)

type PulsarNetServerConfig struct {
	Host      string
	Port      int
	BufferMax int
}

type PulsarNetServerImpl interface {
	ConnectionClosed(conn net.Conn)

	AcceptError(conn net.Conn, err error)
	ReadError(conn net.Conn, err error)
	ReactError(conn net.Conn, err error)
	WriteError(conn net.Conn, err error)

	CommandConnect(conn net.Conn, connect *pb.CommandConnect) (*pb.CommandConnected, error)
	Producer(conn net.Conn, producer *pb.CommandProducer) (*pb.CommandProducerSuccess, error)
	Send(conn net.Conn, send *pb.CommandSend, meta *pb.MessageMetadata, bytes []byte) (*pb.CommandSendReceipt, error)
	CloseProducer(conn net.Conn, closeProducer *pb.CommandCloseProducer) (*pb.CommandSuccess, error)
	PartitionedMetadata(conn net.Conn, partitionedMetadata *pb.CommandPartitionedTopicMetadata) (*pb.CommandPartitionedTopicMetadataResponse, error)
	Lookup(conn net.Conn, lookup *pb.CommandLookupTopic) (*pb.CommandLookupTopicResponse, error)
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
			if isEof(err) {
				p.impl.ConnectionClosed(pulsarConn.conn)
			} else {
				p.impl.ReadError(pulsarConn.conn, err)
				p.impl.ConnectionClosed(pulsarConn.conn)
			}
			break
		}
		pulsarConn.buffer.cursor += readLen
		if pulsarConn.buffer.cursor < 4 {
			continue
		}
		length := codec.FourByteLength(pulsarConn.buffer.bytes) + 4
		if pulsarConn.buffer.cursor < length {
			continue
		}
		if length > pulsarConn.buffer.max {
			p.impl.ReadError(pulsarConn.conn, fmt.Errorf("message too long: %d", length))
			break
		}
		dstBytes, err := p.react(pulsarConn, pulsarConn.buffer.bytes[:length])
		if err != nil {
			p.impl.ReactError(pulsarConn.conn, err)
			p.impl.ConnectionClosed(pulsarConn.conn)
			break
		}
		write, err := pulsarConn.conn.Write(dstBytes)
		if err != nil {
			p.impl.WriteError(pulsarConn.conn, err)
			p.impl.ConnectionClosed(pulsarConn.conn)
			break
		}
		if write != len(dstBytes) {
			p.impl.WriteError(pulsarConn.conn, fmt.Errorf("write %d bytes, but expect %d bytes", write, len(dstBytes)))
			p.impl.ConnectionClosed(pulsarConn.conn)
			break
		}
		pulsarConn.buffer.cursor -= length
		copy(pulsarConn.buffer.bytes[:pulsarConn.buffer.cursor], pulsarConn.buffer.bytes[length:])
	}
}

func (p *PulsarNetServer) react(pulsarConn *pulsarConn, bytes []byte) ([]byte, error) {
	req := &pb.BaseCommand{}
	unmarshalLen := codec.FourByteLength(pulsarConn.buffer.bytes[4:])
	err := proto.Unmarshal(bytes[8:unmarshalLen+8], req)
	if err != nil {
		return nil, err
	}
	var baseCommand *pb.BaseCommand
	switch req.GetType() {
	case pb.BaseCommand_CONNECT:
		commandConnected, err := p.impl.CommandConnect(pulsarConn.conn, req.Connect)
		if err != nil {
			return nil, err
		}
		baseCommand = &pb.BaseCommand{
			Type:      pb.BaseCommand_CONNECTED.Enum(),
			Connected: commandConnected,
		}
	case pb.BaseCommand_PRODUCER:
		commandProducerSuccess, err := p.impl.Producer(pulsarConn.conn, req.Producer)
		if err != nil {
			return nil, err
		}
		baseCommand = &pb.BaseCommand{
			Type:            pb.BaseCommand_PRODUCER_SUCCESS.Enum(),
			ProducerSuccess: commandProducerSuccess,
		}
	case pb.BaseCommand_SEND:
		messageMetaIdx := unmarshalLen + 8
		if bytes[messageMetaIdx+0] == 0x0e {
			if bytes[messageMetaIdx+1] == 0x01 {
				// skip crc32
				messageMetaIdx += 6
			} else if bytes[messageMetaIdx+1] == 0x02 {
				return nil, fmt.Errorf("not support 0e02 yet")
			}
		}
		messageMetaLen := codec.FourByteLength(bytes[messageMetaIdx:])
		messageMeta := &pb.MessageMetadata{}
		err := proto.Unmarshal(bytes[messageMetaIdx+4:messageMetaIdx+4+messageMetaLen], messageMeta)
		if err != nil {
			return nil, err
		}
		commandSendReceipt, err := p.impl.Send(pulsarConn.conn, req.Send, messageMeta, bytes[messageMetaIdx+4+messageMetaLen:])
		if err != nil {
			return nil, err
		}
		baseCommand = &pb.BaseCommand{
			Type:        pb.BaseCommand_SEND_RECEIPT.Enum(),
			SendReceipt: commandSendReceipt,
		}
	case pb.BaseCommand_CLOSE_PRODUCER:
		success, err := p.impl.CloseProducer(pulsarConn.conn, req.CloseProducer)
		if err != nil {
			return nil, err
		}
		baseCommand = &pb.BaseCommand{
			Type:    pb.BaseCommand_SUCCESS.Enum(),
			Success: success,
		}
	case pb.BaseCommand_PARTITIONED_METADATA:
		commandPartitionedTopicMetadataResponse, err := p.impl.PartitionedMetadata(pulsarConn.conn, req.PartitionMetadata)
		if err != nil {
			return nil, err
		}
		baseCommand = &pb.BaseCommand{
			Type:                      pb.BaseCommand_PARTITIONED_METADATA_RESPONSE.Enum(),
			PartitionMetadataResponse: commandPartitionedTopicMetadataResponse,
		}
	case pb.BaseCommand_LOOKUP:
		commandLookupTopicResponse, err := p.impl.Lookup(pulsarConn.conn, req.LookupTopic)
		if err != nil {
			return nil, err
		}
		baseCommand = &pb.BaseCommand{
			Type:                pb.BaseCommand_LOOKUP_RESPONSE.Enum(),
			LookupTopicResponse: commandLookupTopicResponse,
		}
	}
	if baseCommand == nil {
		return nil, fmt.Errorf("unknown command type: %s", req.GetType().String())
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
