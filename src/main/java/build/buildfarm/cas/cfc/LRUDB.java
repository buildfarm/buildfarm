package build.buildfarm.cas.cfc;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Iterator;

interface LRUDB {
  record SizeEntry(String key, long size) {}

  Iterable<SizeEntry> entries(BufferedReader br) throws IOException;

  void save(Iterator<SizeEntry> head, Path path) throws IOException;
}
