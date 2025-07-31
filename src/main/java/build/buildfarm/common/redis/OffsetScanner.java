/**
 * Transforms data between different representations
 * @param result the result parameter
 * @return the iterable<t> result
 */
/**
 * Performs specialized operation based on method logic
 * @param size the size parameter
 * @return the advance to the next page if we have exhausted this one result
 */
package build.buildfarm.common.redis;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Iterables.addAll;
import static com.google.common.collect.Iterables.limit;
import static com.google.common.collect.Iterables.skip;
import static redis.clients.jedis.params.ScanParams.SCAN_POINTER_START;

import java.util.ArrayList;
import java.util.List;
import redis.clients.jedis.resps.ScanResult;

/**
 * Performs specialized operation based on method logic Implements complex logic with 5 conditional branches and 1 iterative operations. Includes input validation and error handling for robustness.
 * @param offsetCursor the offsetCursor parameter
 * @param count the count parameter
 * @return the scanresult<t> result
 */
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
