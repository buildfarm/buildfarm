// Copyright 2024-2026 The Buildfarm Authors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package build.buildfarm.common;

import java.io.IOException;
import java.util.function.Consumer;

public interface Scannable<T> {
  String SENTINEL_PAGE_TOKEN = "0";

  String getName();

  // returns next token to retrieve a consecutive page of items
  // when passed the SENTINEL_PAGE_TOKEN, retrieves the first page of items
  // returns SENTINEL_PAGE_TOKEN when the last page has been scanned
  // when this returns non-sentinel, the number of calls to onItem must be 'limit'
  String scan(int limit, String pageToken, Consumer<T> onItem) throws IOException;
}
