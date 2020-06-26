// Copyright 2020 The Bazel Authors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package build.buildfarm.cas;

import static com.google.common.io.MoreFiles.asCharSink;
import static com.google.common.io.MoreFiles.asCharSource;

import build.bazel.remote.execution.v2.Digest;
import build.buildfarm.common.DigestUtil;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Ephemeral file manifestations of the entry/directory mappings Directory entries are stored in
 * files (and expected to be immutable) Entry directories are maintained in sqlite.
 *
 * <p>Sqlite db should be removed prior to using this index
 */
class FileDirectoriesIndex implements DirectoriesIndex {
  public static final Logger logger = Logger.getLogger(FileDirectoriesIndex.class.getName());

  protected static final String DEFAULT_DIRECTORIES_INDEX_NAME = "directories.sqlite";
  protected static final String DIRECTORIES_INDEX_NAME_MEMORY = ":memory:";

  private static final Charset UTF_8 = Charset.forName("UTF-8");
  private static final int DEFAULT_NUM_OF_DB = 100;

  private final Path root;
  private final int numOfdb;

  private String[] dbUrls;
  private boolean[] isOpen;
  private Connection[] conns;

  FileDirectoriesIndex(String directoriesIndexDbName, Path root, int numOfdb) {
    this.root = root;
    this.numOfdb = numOfdb;
    this.dbUrls = new String[this.numOfdb];
    String directoriesIndexUrl = "jdbc:sqlite:";
    if (directoriesIndexDbName.equals(DIRECTORIES_INDEX_NAME_MEMORY)) {
      directoriesIndexUrl += directoriesIndexDbName;
      Arrays.fill(dbUrls, directoriesIndexUrl);
    } else {
      // db is ephemeral for now, no reuse occurs to match it, computation
      // occurs each time anyway, and expected use of put is noop on collision
      for (int i = 0; i < dbUrls.length; i++) {
        Path path = root.resolve(directoriesIndexDbName + i);
        try {
          if (Files.exists(path)) {
            Files.delete(path);
          }
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
        dbUrls[i] = directoriesIndexUrl + path.toString();
      }
    }

    isOpen = new boolean[this.numOfdb];
    conns = new Connection[this.numOfdb];
    open();
  }

  FileDirectoriesIndex(String dbUrl, Path root) {
    this(dbUrl, root, DEFAULT_NUM_OF_DB);
  }

  private void open() {
    for (int i = 0; i < isOpen.length; i++) {
      if (!isOpen[i]) {
        try {
          logger.log(Level.WARNING, "Creating: " + i + " -> " + dbUrls[i]);
          conns[i] = DriverManager.getConnection(dbUrls[i]);
          try (Statement safetyStatement = conns[i].createStatement()) {
            safetyStatement.execute("PRAGMA synchronous=OFF");
            safetyStatement.execute("PRAGMA journal_mode=OFF");
            safetyStatement.execute("PRAGMA cache_size=100000");
          }
        } catch (SQLException e) {
          throw new RuntimeException(e);
        }

        String createEntriesSql =
            "CREATE TABLE entries (\n"
                + "    path TEXT NOT NULL,\n"
                + "    directory TEXT NOT NULL\n"
                + ")";

        try (Statement stmt = conns[i].createStatement()) {
          logger.log(Level.WARNING, "Creating: " + dbUrls[i]);
          stmt.execute(createEntriesSql);
        } catch (SQLException e) {
          throw new RuntimeException(e);
        }

        isOpen[i] = true;
      }
    }
  }

  @Override
  public synchronized void start() {
    open();

    String createIndexSql = "CREATE INDEX path_idx ON entries (path)";
    int nThread = Runtime.getRuntime().availableProcessors();
    String threadNameFormat = "create-sqlite-index-%d";
    ExecutorService pool = Executors.newFixedThreadPool(
        nThread, new ThreadFactoryBuilder().setNameFormat(threadNameFormat).build()
    );

    for (Connection conn : conns) {
      pool.execute(
          () -> {
            try (Statement stmt = conn.createStatement()) {
              stmt.execute(createIndexSql);
            } catch (SQLException e) {
              throw new RuntimeException(e);
            }
          }
      );
    }
  }

  @Override
  public void close() {
    for (int i = 0; i < conns.length; i++) {
      try {
        conns[i].close();
      } catch (SQLException e) {
        throw new RuntimeException(e);
      }
      isOpen[i] = false;
    }
  }

  // The method should be GuardedBy(conns[Math.abs(entry.hashCode()) % DATABASE_NUMBER]).
  private Set<Digest> removeEntryDirectories(String entry) {
    open();

    Connection conn = conns[Math.abs(entry.hashCode()) % numOfdb];
    String selectSql = "SELECT directory FROM entries WHERE path = ?";
    String deleteSql = "DELETE FROM entries where path = ?";

    ImmutableSet.Builder<Digest> directories = ImmutableSet.builder();
    try (PreparedStatement selectStatement = conn.prepareStatement(selectSql);
        PreparedStatement deleteStatement = conn.prepareStatement(deleteSql)) {
      selectStatement.setString(1, entry);
      try (ResultSet rs = selectStatement.executeQuery()) {
        while (rs.next()) {
          directories.add(DigestUtil.parseDigest(rs.getString("directory")));
        }
      }
      deleteStatement.setString(1, entry);
      deleteStatement.executeUpdate();
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
    return directories.build();
  }

  Path path(Digest digest) {
    return root.resolve(digest.getHash() + "_" + digest.getSizeBytes() + "_dir_inputs");
  }

  @Override
  public Set<Digest> removeEntry(String entry) {
    int dbIndex = Math.abs(entry.hashCode()) % numOfdb;
    Set<Digest> directories;
    synchronized (conns[dbIndex]) {
      directories = removeEntryDirectories(entry);
    }
    try {
      for (Digest directory : directories) {
        Files.delete(path(directory));
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return directories;
  }

  @Override
  public Iterable<String> directoryEntries(Digest directory) {
    try {
      return asCharSource(path(directory), UTF_8).readLines();
    } catch (NoSuchFileException e) {
      return ImmutableList.of();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  // The method should be GuardedBy(conns[Math.abs(entry.hashCode()) % DATABASE_NUMBER]).
  private void addEntriesDirectory(String entry, Digest directory) {
    open();

    String digest = DigestUtil.toString(directory);
    String insertSql = "INSERT INTO entries (path, directory)\n" + "    VALUES (?,?)";
    int dbIndex = Math.abs(entry.hashCode()) % numOfdb;
    try (PreparedStatement insertStatement = conns[dbIndex].prepareStatement(insertSql)) {
      conns[dbIndex].setAutoCommit(false);
      insertStatement.setString(2, digest);
      insertStatement.setString(1, entry);
      insertStatement.executeUpdate();
      conns[dbIndex].commit();
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void put(Digest directory, Iterable<String> entries) {
    try {
      asCharSink(path(directory), UTF_8).writeLines(entries);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    Set<String> uniqueEntries = ImmutableSet.copyOf(entries);
    for (String entry : uniqueEntries) {
      synchronized (conns[Math.abs(entry.hashCode()) % numOfdb]) {
        addEntriesDirectory(entry, directory);
      }
    }
  }

  // The method should be GuardedBy(conns[Math.abs(entry.hashCode()) % DATABASE_NUMBER]).
  private void removeEntriesDirectory(String entry, Digest directory) {
    open();

    String digest = DigestUtil.toString(directory);
    String deleteSql = "DELETE FROM entries WHERE path = ? AND directory = ?";
    int dbIndex = Math.abs(entry.hashCode()) % numOfdb;
    try (PreparedStatement deleteStatement = conns[dbIndex].prepareStatement(deleteSql)) {
      conns[dbIndex].setAutoCommit(false);
      // safe for multi delete
      deleteStatement.setString(1, entry);
      deleteStatement.setString(2, digest);
      deleteStatement.executeUpdate();
      conns[dbIndex].commit();
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public synchronized void remove(Digest directory) {
    Iterable<String> entries = directoryEntries(directory);
    try {
      Files.delete(path(directory));
    } catch (NoSuchFileException e) {
      // ignore
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    for (String entry : entries) {
      synchronized (conns[Math.abs(entry.hashCode()) % numOfdb]) {
        removeEntriesDirectory(entry, directory);
      }
    }
  }
}
