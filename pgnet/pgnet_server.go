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

package pgnet

import (
	"context"
	"fmt"
	"github.com/gogo/protobuf/proto"
	"github.com/panjf2000/gnet"
	"github.com/protocol-laboratory/pulsar-codec-go/codec"
	"github.com/protocol-laboratory/pulsar-codec-go/pb"
	"runtime/debug"
)

type GnetServerConfig struct {
	ListenHost   string
	ListenPort   int
	EventLoopNum int
}

type PulsarServerImpl interface {
	OnInitComplete(server gnet.Server) (action gnet.Action)
	OnOpened(c gnet.Conn) (out []byte, action gnet.Action)
	OnClosed(c gnet.Conn, err error) (action gnet.Action)
	InvalidPulsarPacket(c gnet.Conn)
	ConnError(c gnet.Conn, err error)
	UnSupportedPulsarPacket(c gnet.Conn)
	Connect(c gnet.Conn, connect *pb.CommandConnect) (*pb.CommandConnected, error)
}

type PulsarServer struct {
	gnetConfig GnetServerConfig
	impl       PulsarServerImpl
	*gnet.EventServer
}

func (p *PulsarServer) OnInitComplete(server gnet.Server) (action gnet.Action) {
	p.impl.OnInitComplete(server)
	return
}

func (p *PulsarServer) React(frame []byte, c gnet.Conn) (_ []byte, action gnet.Action) {
	defer func() {
		if r := recover(); r != nil {
			p.impl.ConnError(c, codec.PanicToError(r, debug.Stack()))
			action = gnet.Close
		}
	}()
	cmd := &pb.BaseCommand{}
	err := proto.Unmarshal(frame[4:], cmd)
	if err != nil {
		p.impl.InvalidPulsarPacket(c)
		return nil, gnet.Close
	}
	switch *cmd.Type {
	case pb.BaseCommand_CONNECT:
		connected, err := p.impl.Connect(c, cmd.Connect)
		if err != nil {
			p.impl.ConnError(c, err)
			return nil, gnet.Close
		}
		marshal, err := pb.MarshalCmd(pb.BaseCommand_CONNECTED, connected, false)
		if err != nil {
			p.impl.ConnError(c, err)
			return nil, gnet.Close
		}
		return marshal, gnet.None
	default:
		break
	}
	p.impl.UnSupportedPulsarPacket(c)
	return nil, gnet.Close
}

func (p *PulsarServer) OnOpened(c gnet.Conn) (out []byte, action gnet.Action) {
	p.impl.OnOpened(c)
	return
}

func (p *PulsarServer) OnClosed(c gnet.Conn, err error) (action gnet.Action) {
	p.impl.OnClosed(c, err)
	return
}

func (p *PulsarServer) Run() error {
	return gnet.Serve(p, fmt.Sprintf("tcp://%s:%d", p.gnetConfig.ListenHost, p.gnetConfig.ListenPort), gnet.WithNumEventLoop(p.gnetConfig.EventLoopNum), gnet.WithCodec(pulsarCodec))
}

func (p *PulsarServer) Stop(ctx context.Context) error {
	addr := fmt.Sprintf("tcp://%s:%d", p.gnetConfig.ListenHost, p.gnetConfig.ListenPort)
	return gnet.Stop(ctx, addr)
}

func NewPulsarServer(gnetConfig GnetServerConfig, impl PulsarServerImpl) *PulsarServer {
	return &PulsarServer{
		gnetConfig: gnetConfig,
		impl:       impl,
	}
}
