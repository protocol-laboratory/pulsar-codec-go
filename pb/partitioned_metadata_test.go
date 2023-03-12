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

func TestDecodePartitionMetadata(t *testing.T) {
	bytes := codec.TestHex2Bytes(t, "0815aa01320a2670657273697374656e743a2f2f7075626c69632f64656661756c742f746573742d746f70696310c2e0ffffe6e9dde10c")
	cmd := &BaseCommand{}
	err := proto.Unmarshal(bytes, cmd)
	if err != nil {
		assert.Nil(t, err)
	}
}
