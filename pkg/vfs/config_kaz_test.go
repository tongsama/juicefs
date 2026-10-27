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

package vfs

import (
	"reflect"
	"testing"
)

// kazAssertEmbeddedNotShadowed fails when outer does not embed a field of
// type embedded, or when a direct field of outer has the same name as a field
// of embedded. A shadowed field would silently stop reaching the kaz value
// (and its .config key), so moving fields into an embedded struct must keep
// the names disjoint.
func kazAssertEmbeddedNotShadowed(t *testing.T, outer, embedded reflect.Type) {
	t.Helper()
	found := false
	direct := make(map[string]bool)
	for i := 0; i < outer.NumField(); i++ {
		f := outer.Field(i)
		if f.Anonymous && f.Type == embedded {
			found = true
			continue
		}
		direct[f.Name] = true
	}
	if !found {
		t.Fatalf("%s does not embed %s", outer, embedded)
	}
	for i := 0; i < embedded.NumField(); i++ {
		if name := embedded.Field(i).Name; direct[name] {
			t.Errorf("%s.%s shadows %s.%s", outer, name, embedded, name)
		}
	}
}

// TestKazEmbeddedFieldsNotShadowed checks that the kaz config and state
// structs stay embedded and that no upstream field hides one of their fields.
func TestKazEmbeddedFieldsNotShadowed(t *testing.T) {
	kazAssertEmbeddedNotShadowed(t, reflect.TypeOf((*Config)(nil)).Elem(), reflect.TypeOf((*KazVfsConfig)(nil)).Elem())
	kazAssertEmbeddedNotShadowed(t, reflect.TypeOf((*fileWriter)(nil)).Elem(), reflect.TypeOf((*kazFileWriterState)(nil)).Elem())
}
