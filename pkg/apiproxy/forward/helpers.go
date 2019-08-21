// Copyright (c) 2019 Uber Technologies, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package forward

import (
	"io"
	"sync"
)

const (
	_copyBufSize = 1024 * 32
)

var _pool = sync.Pool{
	New: func() interface{} {
		return &buffer{make([]byte, _copyBufSize)}
	},
}

type buffer struct {
	b []byte
}

// copy copies bytes from the Reader to the Writer until the Reader is exhausted.
func copy(dst io.Writer, src io.Reader) (int64, error) {
	// To avoid unnecessary memory allocations we maintain our own pool of buffers.
	buf := _pool.Get().(*buffer)
	written, err := io.CopyBuffer(dst, src, buf.b)
	_pool.Put(buf)
	return written, err
}
