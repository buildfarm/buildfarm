// Copyright 2026 The Buildfarm Authors. All rights reserved.
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

package build.buildfarm.fs.ac;

import build.buildfarm.common.io.AtomicFileWriter;
import build.buildfarm.common.io.FileStoreMover;
import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.function.Function;

class TextDB<E> {
  private final FileStoreMover fileStoreMover;
  private final Function<String, E> parser;
  private final Function<E, String> printer;

  TextDB(FileStoreMover fileStoreMover, Function<String, E> parser, Function<E, String> printer) {
    this.fileStoreMover = fileStoreMover;
    this.parser = parser;
    this.printer = printer;
  }

  private class EntriesIterator implements Iterator<E> {
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

    @Override
    public E next() {
      if (next == null) {
        throw new NoSuchElementException();
      }
      E entry = parser.apply(next);
      next = null;
      return entry;
    }
  }

  public Iterable<E> entries(BufferedReader br) throws IOException {
    return new Iterable<>() {
      @Override
      public Iterator<E> iterator() {
        return new EntriesIterator(br);
      }
    };
  }

  public void save(Iterator<E> entries, Path path) throws IOException {
    try (AtomicFileWriter writer = new AtomicFileWriter(path, fileStoreMover)) {
      while (entries.hasNext()) {
        E entry = entries.next();
        writer.write(printer.apply(entry));
      }
      writer.onSuccess();
    }
  }
}
