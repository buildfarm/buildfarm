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

      String createEntriesSql =
          "CREATE TABLE entries (\n"
              + "    path TEXT NOT NULL,\n"
              + "    directory TEXT NOT NULL\n"
              + ")";

      try (Statement stmt = conn.createStatement()) {
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

    String createPathIndexSql = "CREATE INDEX path_idx ON entries (path)";
    String createDirectoryIndexSql = "CREATE INDEX directory_idx ON entries (directory)";
    try (Statement stmt = conn.createStatement()) {
      stmt.execute(createPathIndexSql);
      stmt.execute(createDirectoryIndexSql);
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

    String selectSql = "SELECT directory FROM entries WHERE path = ?";

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
    String deleteSql = "DELETE FROM entries where directory = ?";
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
    return directories;
  }

  @Override
  public synchronized Set<Digest> removeEntry(String entry) throws IOException {
    Set<Digest> directories = removeEntryDirectories(entry);
    super.removeDirectories(directories);
    return directories;
  }

  private synchronized void addEntriesDirectory(Set<String> entries, Digest directory) {
    open();

    String digest = DigestUtil.toString(directory);
    String insertSql = "INSERT INTO entries (path, directory) VALUES (?,?)";
    try (PreparedStatement insertStatement = conn.prepareStatement(insertSql)) {
      conn.setAutoCommit(false);
      insertStatement.setString(2, digest);
      for (String entry : entries) {
        insertStatement.setString(1, entry);
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
    String deleteSql = "DELETE FROM entries WHERE directory = ?";
    try (PreparedStatement deleteStatement = conn.prepareStatement(deleteSql)) {
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
