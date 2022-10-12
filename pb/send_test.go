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
	"github.com/gogo/protobuf/proto"
	"github.com/protocol-laboratory/pulsar-codec-go/codec"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestDecodeSend(t *testing.T) {
	bytes := codec.TestHex2Bytes(t, "0000000a080632060800100030000e013e84e739000000210a0e7374616e64616c6f6e652d302d33100018bfaa8a85df2e5801820100c0010000000004180340006d7367")
	sendLength := codec.FourByteLength(bytes)
	sendCmd := &BaseCommand{}
	err := proto.Unmarshal(bytes[4:sendLength+4], sendCmd)
	if err != nil {
		assert.Nil(t, err)
	}
	messageMetaIdx := sendLength + 4
	if bytes[messageMetaIdx+0] == 0x0e {
		if bytes[messageMetaIdx+1] == 0x01 {
			// skip crc32
			messageMetaIdx += 6
		} else if bytes[messageMetaIdx+1] == 0x02 {
			panic("not support 0e02 yet")
		}
	}
	metadataLength := codec.FourByteLength(bytes[messageMetaIdx : messageMetaIdx+4])
	metadata := &MessageMetadata{}
	err = proto.Unmarshal(bytes[messageMetaIdx+4:messageMetaIdx+4+metadataLength], metadata)
	if err != nil {
		assert.Nil(t, err)
	}
}
