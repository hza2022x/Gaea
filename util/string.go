// Copyright 2019 The Gaea Authors. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package util

import (
	"bytes"
	"fmt"
)

func Concat(strings ...string) string {
	var buffer bytes.Buffer
	for _, s := range strings {
		buffer.WriteString(s)
	}
	return buffer.String()
}

func GetOnOffVariable(v string) (string, error) {
	if v == "1" || v == "on" {
		return "1", nil
	} else if v == "0" || v == "off" {
		return "0", nil
	} else {
		return "", fmt.Errorf("not an on off string")
	}
}
