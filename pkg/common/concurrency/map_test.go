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

package concurrency

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"github.com/uber/peloton/pkg/common/concurrency/mocks"
)

func TestMap_Success(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mapper := mocks.NewMockMapper(ctrl)

	var inputs []interface{}
	var expected []interface{}
	for i := 0; i < 1000; i++ {
		inputs = append(inputs, i)
		output := fmt.Sprintf("output %d", i)
		expected = append(expected, output)
		mapper.EXPECT().Map(gomock.Any(), i).Return(output, nil)
	}

	outputs, err := Map(context.Background(), mapper, inputs, 25)
	require.NoError(t, err)
	require.ElementsMatch(t, expected, outputs)
}

func TestMap_ErrorStopsEarly(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mapper := mocks.NewMockMapper(ctrl)

	var inputs []interface{}
	for i := 0; i < 1000; i++ {
		inputs = append(inputs, i)
	}

	expectedErr := errors.New("some error")

	// The first error will hit and send to consumer, and then the consumer
	// should begin teardown, meaning the second error (if reached) will block
	// on send until ctx is cancelled by consumer.
	mapper.EXPECT().Map(gomock.Any(), int(0)).Return(nil, expectedErr)
	mapper.EXPECT().Map(gomock.Any(), int(1)).Return(nil, expectedErr).MaxTimes(1)

	outputs, err := Map(context.Background(), mapper, inputs, 1)
	require.Equal(t, expectedErr, err)
	require.Nil(t, outputs)
}

func TestMap_PartialErrors(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mapper := mocks.NewMockMapper(ctrl)

	var inputs []interface{}
	for i := 0; i < 1000; i++ {
		inputs = append(inputs, i)

		// Return error on 25% of inputs.
		if i%4 == 0 {
			mapper.EXPECT().
				Map(gomock.Any(), i).
				Return(nil, errors.New("some error")).
				MaxTimes(1)
		} else {
			mapper.EXPECT().
				Map(gomock.Any(), i).
				Return(fmt.Sprintf("output %d", i), nil).
				MaxTimes(1)
		}
	}

	outputs, err := Map(context.Background(), mapper, inputs, 25)
	require.Error(t, err)
	require.Nil(t, outputs)
}

func TestMap_ContextTimeout(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mapper := mocks.NewMockMapper(ctrl)

	timeout := time.Second

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	var inputs []interface{}
	for i := 0; i < 1000; i++ {
		inputs = append(inputs, i)
		mapper.EXPECT().
			Map(gomock.Any(), i).
			DoAndReturn(func(ctx context.Context, input interface{}) (interface{}, error) {
				// Each mapper should be cancelled when the context times out.
				select {
				case <-time.After(timeout * 2):
					require.FailNow(t, "Mapper should not timeout")
					return nil, nil
				case <-ctx.Done():
					return nil, ctx.Err()
				}
			}).
			MaxTimes(1)
	}

	outputs, err := Map(ctx, mapper, inputs, 25)
	require.Error(t, err)
	require.Nil(t, outputs)
}
