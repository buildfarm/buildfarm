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
