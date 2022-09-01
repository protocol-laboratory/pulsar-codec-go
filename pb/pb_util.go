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

package pb

import (
	"encoding/binary"
	"fmt"
	"github.com/gogo/protobuf/proto"
)

func NewCommand(cmdType BaseCommand_Type, msg proto.Message) *BaseCommand {
	cmd := &BaseCommand{
		Type: &cmdType,
	}
	switch cmdType {
	case BaseCommand_CONNECT:
		cmd.Connect = msg.(*CommandConnect)
	case BaseCommand_CONNECTED:
		cmd.Connected = msg.(*CommandConnected)
	case BaseCommand_LOOKUP:
		cmd.LookupTopic = msg.(*CommandLookupTopic)
	case BaseCommand_LOOKUP_RESPONSE:
		cmd.LookupTopicResponse = msg.(*CommandLookupTopicResponse)
	case BaseCommand_PARTITIONED_METADATA:
		cmd.PartitionMetadata = msg.(*CommandPartitionedTopicMetadata)
	case BaseCommand_PRODUCER:
		cmd.Producer = msg.(*CommandProducer)
	case BaseCommand_SUBSCRIBE:
		cmd.Subscribe = msg.(*CommandSubscribe)
	case BaseCommand_FLOW:
		cmd.Flow = msg.(*CommandFlow)
	case BaseCommand_PING:
		cmd.Ping = msg.(*CommandPing)
	case BaseCommand_PONG:
		cmd.Pong = msg.(*CommandPong)
	case BaseCommand_SEND:
		cmd.Send = msg.(*CommandSend)
	case BaseCommand_SEND_ERROR:
		cmd.SendError = msg.(*CommandSendError)
	case BaseCommand_CLOSE_PRODUCER:
		cmd.CloseProducer = msg.(*CommandCloseProducer)
	case BaseCommand_CLOSE_CONSUMER:
		cmd.CloseConsumer = msg.(*CommandCloseConsumer)
	case BaseCommand_ACK:
		cmd.Ack = msg.(*CommandAck)
	case BaseCommand_SEEK:
		cmd.Seek = msg.(*CommandSeek)
	case BaseCommand_UNSUBSCRIBE:
		cmd.Unsubscribe = msg.(*CommandUnsubscribe)
	case BaseCommand_REDELIVER_UNACKNOWLEDGED_MESSAGES:
		cmd.RedeliverUnacknowledgedMessages = msg.(*CommandRedeliverUnacknowledgedMessages)
	case BaseCommand_GET_TOPICS_OF_NAMESPACE:
		cmd.GetTopicsOfNamespace = msg.(*CommandGetTopicsOfNamespace)
	case BaseCommand_GET_LAST_MESSAGE_ID:
		cmd.GetLastMessageId = msg.(*CommandGetLastMessageId)
	case BaseCommand_AUTH_RESPONSE:
		cmd.AuthResponse = msg.(*CommandAuthResponse)
	default:
		panic(fmt.Sprintf("Missing command type: %v", cmdType))
	}
	return cmd
}

func MarshalBaseCmd(cmd *BaseCommand, containPulsarLen bool) ([]byte, error) {
	size := cmd.Size()
	var bytes []byte
	var marshalIdx int
	if containPulsarLen {
		bytes = make([]byte, size+8)
		marshalIdx = 8
	} else {
		bytes = make([]byte, size+4)
		marshalIdx = 4
	}
	_, err := cmd.MarshalTo(bytes[marshalIdx:])
	if err != nil {
		return nil, err
	}
	if containPulsarLen {
		binary.BigEndian.PutUint32(bytes, uint32(size)+4)
		binary.BigEndian.PutUint32(bytes[4:], uint32(size))
	} else {
		binary.BigEndian.PutUint32(bytes, uint32(size))
	}
	return bytes, nil
}

func MarshalCmd(cmdType BaseCommand_Type, msg proto.Message, containPulsarLen bool) ([]byte, error) {
	return MarshalBaseCmd(NewCommand(cmdType, msg), containPulsarLen)
}
