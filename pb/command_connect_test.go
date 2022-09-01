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

func TestDecodeCommandConnect(t *testing.T) {
	bytes := codec.TestHex2Bytes(t, "0802121a0a06322e31302e301a0020132a046e6f6e655206080110011801")
	cmd := &BaseCommand{}
	err := proto.Unmarshal(bytes, cmd)
	if err != nil {
		assert.Nil(t, err)
	}
}
