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

package codec

import "encoding/binary"

// int

func ReadInt(bytes []byte, idx int) (int, int) {
	return int(binary.BigEndian.Uint32(bytes[idx:])), idx + 4
}

func PutInt(bytes []byte, idx int, x int) int {
	return PutUInt32(bytes, idx, uint32(x))
}

func ReadInt32(bytes []byte, idx int) (int32, int) {
	return int32(binary.BigEndian.Uint32(bytes[idx:])), idx + 4
}

func PutInt32(bytes []byte, idx int, x int32) int {
	return PutUInt32(bytes, idx, uint32(x))
}

// uint

func PutUInt32(bytes []byte, idx int, x uint32) int {
	binary.BigEndian.PutUint32(bytes[idx:idx+4], x)
	return idx + 4
}

func ReadUInt32(bytes []byte, idx int) (uint32, int) {
	return binary.BigEndian.Uint32(bytes[idx:]), idx + 4
}
