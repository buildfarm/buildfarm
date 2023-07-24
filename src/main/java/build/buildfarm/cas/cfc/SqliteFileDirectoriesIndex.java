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

package build.buildfarm.cas.cfc;

import build.bazel.remote.execution.v2.Digest;
import build.buildfarm.common.DigestUtil;
import com.google.common.collect.ImmutableSet;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Set;
import javax.annotation.concurrent.GuardedBy;

/**
 * Ephemeral file manifestations of the entry/directory mappings Directory entries are stored in
 * files (and expected to be immutable) Entry directories are maintained in sqlite.
 *
 * <p>Sqlite db should be removed prior to using this index
 */
class SqliteFileDirectoriesIndex extends FileDirectoriesIndex {
  private final String dbUrl;
  private boolean opened = false;
  private Connection conn;

  SqliteFileDirectoriesIndex(String dbUrl, EntryPathStrategy entryPathStrategy) {
    super(entryPathStrategy);
    this.dbUrl = dbUrl;
  }

  @GuardedBy("this")
  private void open() {
    if (!opened) {
      try {
        conn = DriverManager.getConnection(dbUrl);
        try (Statement safetyStatement = conn.createStatement()) {
          safetyStatement.execute("PRAGMA synchronous=OFF");
          safetyStatement.execute("PRAGMA journal_mode=OFF");
          safetyStatement.execute("PRAGMA cache_size=100000");
        }
      } catch (SQLException e) {
        throw new RuntimeException(e);
      }

      String createDirectoriesSql =
          "CREATE TABLE directories (id INTEGER PRIMARY KEY, name VARCHAR UNIQUE)";
      String createFilesSql = "CREATE TABLE files (id INTEGER PRIMARY KEY, name VARCHAR UNIQUE)";
      String createEntriesSql =
          "CREATE TABLE entries (\n"
              + "    file_id INTEGER NOT NULL REFERENCES files(id) ON DELETE CASCADE,\n"
              + "    directory_id INTEGER NOT NULL REFERENCES directories(id) ON DELETE CASCADE,\n"
              + "    PRIMARY KEY (file_id, directory_id)\n"
              + ")";

      try (Statement stmt = conn.createStatement()) {
        stmt.execute(createDirectoriesSql);
        stmt.execute(createFilesSql);
        stmt.execute(createEntriesSql);
      } catch (SQLException e) {
        throw new RuntimeException(e);
      }

      opened = true;
    }
  }

  @Override
  public synchronized void start() {
    open();

    String createPathIndexSql = "CREATE INDEX file_idx ON entries (file_id)";
    String createDirectoryIndexSql = "CREATE INDEX directory_idx ON entries (directory_id)";
    String enforceForeignKeys = "PRAGMA foreign_keys=ON";
    try (Statement stmt = conn.createStatement()) {
      stmt.execute(createPathIndexSql);
      stmt.execute(createDirectoryIndexSql);
      stmt.execute(enforceForeignKeys);
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void close() {
    try {
      conn.close();
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
    opened = false;
  }

  @GuardedBy("this")
  private Set<Digest> removeEntryDirectories(String entry) {
    open();

    String selectSql =
        "SELECT d.name as directory FROM files f INNER JOIN entries e ON f.id = e.file_id INNER JOIN directories d ON d.id = e.directory_id WHERE f.name = ?";

    ImmutableSet.Builder<Digest> directoriesBuilder = ImmutableSet.builder();
    try (PreparedStatement selectStatement = conn.prepareStatement(selectSql)) {
      selectStatement.setString(1, entry);
      try (ResultSet rs = selectStatement.executeQuery()) {
        while (rs.next()) {
          directoriesBuilder.add(DigestUtil.parseDigest(rs.getString("directory")));
        }
      }
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
    // all directories featuring this entry are now invalid
    ImmutableSet<Digest> directories = directoriesBuilder.build();
    String deleteSql = "DELETE FROM directories where name = ?";
    try (PreparedStatement deleteStatement = conn.prepareStatement(deleteSql)) {
      conn.setAutoCommit(false);
      for (Digest directory : directories) {
        deleteStatement.setString(1, DigestUtil.toString(directory));
        deleteStatement.addBatch();
      }
      deleteStatement.executeBatch();
      conn.commit();
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
    // clear out orphaned files
    try (Statement orphanStatement = conn.createStatement()) {
      String deleteOrphanSql =
          "DELETE FROM files WHERE id NOT IN (SELECT DISTINCT file_id FROM entries)";
      orphanStatement.execute(deleteOrphanSql);
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
    return directories;
  }

  @Override
  public synchronized Set<Digest> removeEntry(String entry) throws IOException {
    Set<Digest> directories = removeEntryDirectories(entry);
    super.removeDirectories(directories);
    return directories;
  }

  // inserts here specifically avoids integer key maintenance in java
  private synchronized void addEntriesDirectory(Set<String> entries, Digest directory) {
    open();

    String directoryName = DigestUtil.toString(directory);
    String filesInsertSql = "INSERT OR IGNORE INTO files (name) VALUES (?)";
    try (PreparedStatement filesInsertStatement = conn.prepareStatement(filesInsertSql)) {
      conn.setAutoCommit(false);
      for (String entry : entries) {
        filesInsertStatement.setString(1, entry);
        filesInsertStatement.addBatch();
      }
      filesInsertStatement.executeBatch();
      conn.commit();
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
    // should be novel directory
    String directoriesInsertSql = "INSERT INTO directories (name) VALUES (?)";
    try (PreparedStatement directoriesInsertStatement =
        conn.prepareStatement(directoriesInsertSql)) {
      conn.setAutoCommit(false);
      directoriesInsertStatement.setString(1, directoryName);
      directoriesInsertStatement.executeUpdate();
      conn.commit();
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
    String entriesInsertSql =
        "INSERT INTO entries (file_id, directory_id) SELECT f.id, d.id FROM files f, directories d WHERE f.name = ? AND d.name = ?";
    try (PreparedStatement insertStatement = conn.prepareStatement(entriesInsertSql)) {
      conn.setAutoCommit(false);
      for (String entry : entries) {
        insertStatement.setString(1, entry);
        insertStatement.setString(2, directoryName);
        insertStatement.addBatch();
      }
      insertStatement.executeBatch();
      conn.commit();
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void put(Digest directory, Iterable<String> entries) throws IOException {
    super.put(directory, entries);
    addEntriesDirectory(ImmutableSet.copyOf(entries), directory);
  }

  @GuardedBy("this")
  private void removeEntriesDirectory(Digest directory) {
    open();

    String digest = DigestUtil.toString(directory);
    String deleteSql = "DELETE FROM directories WHERE name = ?";
    try (PreparedStatement deleteStatement = conn.prepareStatement(deleteSql)) {
      conn.setAutoCommit(true);
      deleteStatement.setString(1, digest);
      deleteStatement.executeUpdate();
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public synchronized void remove(Digest directory) throws IOException {
    super.remove(directory);
    removeEntriesDirectory(directory);
  }
}
