/*
 * JuiceFS, Copyright 2026 Juicedata, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package meta

import (
	"testing"
	"time"
)

// TestKazCompactionPolicyConfig checks the ranges, the zero-value defaults and the SelfCheck fallback.
func TestKazCompactionPolicyConfig(t *testing.T) {
	for _, bound := range []int{kazCompactionMaxReadMin, kazCompactionMaxForce} {
		if bound != maxSlices-1 {
			t.Fatalf("policy bound %d must stay just below maxSlices %d", bound, maxSlices)
		}
	}
	var zero Config
	if err := zero.ValidateKazCompactionPolicy(); err != nil {
		t.Fatalf("zero config rejected: %v", err)
	}
	if r, cd, f := zero.kazCompactionSettings(); r != 5 || cd != 0 || f != 1000 {
		t.Fatalf("zero settings = %d/%s/%d, want 5/0s/1000", r, cd, f)
	}
	if zero.kazCompactionPolicyEnabled() {
		t.Fatal("zero config must keep the upstream behavior")
	}
	for _, c := range []Config{
		{KazCompactionReadMin: 5}, {KazCompactionReadMin: 2499},
		{KazCompactionCooldown: time.Second}, {KazCompactionCooldown: time.Hour},
		{KazCompactionForceSlices: 100}, {KazCompactionForceSlices: 2499},
	} {
		if err := c.ValidateKazCompactionPolicy(); err != nil {
			t.Errorf("%+v rejected: %v", c, err)
		}
	}
	for _, c := range []Config{
		{KazCompactionReadMin: -1}, {KazCompactionReadMin: 4}, {KazCompactionReadMin: 2500},
		{KazCompactionCooldown: -time.Second}, {KazCompactionCooldown: 999 * time.Millisecond}, {KazCompactionCooldown: time.Hour + time.Second},
		{KazCompactionForceSlices: -1}, {KazCompactionForceSlices: 99}, {KazCompactionForceSlices: 2500},
	} {
		if err := c.ValidateKazCompactionPolicy(); err == nil {
			t.Errorf("%+v accepted", c)
		}
	}
	for _, tc := range []struct {
		c    Config
		want bool
	}{
		{Config{KazCompactionReadMin: 5}, false},
		{Config{KazCompactionReadMin: 100}, true},
		{Config{KazCompactionCooldown: time.Minute}, true},
		{Config{KazCompactionForceSlices: 700}, false},
	} {
		if got := tc.c.kazCompactionPolicyEnabled(); got != tc.want {
			t.Errorf("%+v enabled=%v, want %v", tc.c, got, tc.want)
		}
	}
	bad := Config{KazCompactionReadMin: 3, KazCompactionCooldown: time.Minute, KazCompactionForceSlices: 700, MaxDeletes: 1}
	bad.SelfCheck()
	if bad.KazCompactionReadMin != 0 || bad.KazCompactionCooldown != 0 || bad.KazCompactionForceSlices != 0 {
		t.Fatalf("SelfCheck kept invalid policy: %+v", bad)
	}
}
