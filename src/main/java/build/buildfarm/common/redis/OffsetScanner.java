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

package build.buildfarm.common.redis;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Iterables.addAll;
import static com.google.common.collect.Iterables.limit;
import static com.google.common.collect.Iterables.skip;
import static redis.clients.jedis.params.ScanParams.SCAN_POINTER_START;

import java.util.ArrayList;
import java.util.List;
import redis.clients.jedis.resps.ScanResult;

public abstract class OffsetScanner<T> {
  protected abstract ScanResult<T> scan(String cursor, int remaining);

  protected Iterable<T> transform(Iterable<T> result) {
    return result;
  }

  public ScanResult<T> fill(String offsetCursor, int count) {
    int offsetIndex = offsetCursor.indexOf('+');
    int offset = 0;
    String cursor = offsetCursor;
    if (offsetIndex != -1) {
      cursor = offsetCursor.substring(0, offsetIndex);
      offset = Integer.parseInt(offsetCursor.substring(offsetIndex + 1));
      checkState(offset >= 0);
    }
    List<T> result = new ArrayList<>(count);
    boolean atEnd = false;
    while (count > 0 && !atEnd) {
      ScanResult<T> scanResult = scan(cursor, count);
      int size = scanResult.getResult().size();
      addAll(result, transform(limit(skip(scanResult.getResult(), offset), count)));
      int available = Math.min(count, size - offset);
      offset += available;
      count -= available;

      // advance to the next page if we have exhausted this one
      if (offset == size) {
        offset = 0;
        cursor = scanResult.getCursor();
      }
      // last page means that we're done with this loop
      if (scanResult.getCursor().equals(SCAN_POINTER_START)) {
        atEnd = true;
      }
    }
    String nextCursor = cursor + "+" + offset;
    if (atEnd && offset == 0) {
      nextCursor = SCAN_POINTER_START;
    }
    return new ScanResult(nextCursor, result);
  }
}
