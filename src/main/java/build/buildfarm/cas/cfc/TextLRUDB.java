// Copyright 2025-2026 The Buildfarm Authors. All rights reserved.
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

package build.buildfarm.cas.cfc;

import static com.google.common.base.Preconditions.checkState;
import static java.lang.String.format;

import build.buildfarm.common.io.AtomicFileWriter;
import build.buildfarm.common.io.FileStoreMover;
import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.NoSuchElementException;

class TextLRUDB implements LRUDB {
  private final FileStoreMover fileStoreMover;

  TextLRUDB(FileStoreMover fileStoreMover) {
    this.fileStoreMover = fileStoreMover;
  }

  private static class EntriesIterator implements Iterator<SizeEntry> {
    private String next = null;
    private final BufferedReader br;

    EntriesIterator(BufferedReader br) {
      this.br = br;
    }

    @Override
    public boolean hasNext() {
      if (next == null) {
        try {
          next = br.readLine();
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }
      return next != null;
    }

    private SizeEntry parse(String line) {
      int sep = line.indexOf(',');
      checkState(sep != -1);
      return new SizeEntry(line.substring(0, sep), Long.parseLong(line.substring(sep + 1)));
    }

    @Override
    public SizeEntry next() {
      if (next == null) {
        throw new NoSuchElementException();
      }
      SizeEntry sizeEntry = parse(next);
      next = null;
      return sizeEntry;
    }
  }

  @Override
  public Iterable<SizeEntry> entries(BufferedReader br) throws IOException {
    return new Iterable<>() {
      @Override
      public Iterator<SizeEntry> iterator() {
        return new EntriesIterator(br);
      }
    };
  }

  @Override
  public void save(Iterator<SizeEntry> entries, Path path) throws IOException {
    try (AtomicFileWriter writer = new AtomicFileWriter(path, fileStoreMover)) {
      while (entries.hasNext()) {
        SizeEntry entry = entries.next();
        writer.write(format("%s,%d\n", entry.key(), entry.size()));
      }
      writer.onSuccess();
    }
  }
}
