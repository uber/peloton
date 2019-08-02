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

package adminsvc

import (
	"github.com/uber/peloton/.gen/peloton/api/v1alpha/admin/svc"
	"github.com/uber/peloton/pkg/jobmgr/task/lifecyclemgr"
)

type killLockComponent struct {
	lockable lifecyclemgr.Lockable
}

func (c *killLockComponent) lock() error {
	c.lockable.LockKill()
	return nil
}

func (c *killLockComponent) unlock() error {
	c.lockable.UnlockKill()
	return nil
}

func (c *killLockComponent) component() svc.Component {
	return svc.Component_Kill
}

type launchLockComponent struct {
	lockable lifecyclemgr.Lockable
}

func (c *launchLockComponent) lock() error {
	c.lockable.LockLaunch()
	return nil
}

func (c *launchLockComponent) unlock() error {
	c.lockable.UnlockLaunch()
	return nil
}

func (c *launchLockComponent) component() svc.Component {
	return svc.Component_Launch
}
