package build.buildfarm.common;

import static com.google.common.collect.Iterables.skip;

import java.util.function.Consumer;

public final class IterableScannable<T> implements Scannable<T> {
  private final String name;
  private final Iterable<T> iterable;

  public IterableScannable(String name, Iterable<T> iterable) {
    this.name = name;
    this.iterable = iterable;
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public String scan(int limit, String pageToken, Consumer<T> onItem) {
    if (limit <= 0) {
      return pageToken;
    }
    int index = pageToken.isEmpty() ? 0 : Integer.parseInt(pageToken);
    for (T item : skip(iterable, index)) {
      if (limit-- <= 0) {
        break;
      }
      index++;
      onItem.accept(item);
    }
    return limit < 0 ? Integer.toString(index) : SENTINEL_PAGE_TOKEN;
  }
}
