/**
 * Performs specialized operation based on method logic
 * @return the iterator<entry> result
 */
package build.buildfarm.worker;

import static com.google.common.collect.Iterators.concat;
import static com.google.common.collect.Iterators.transform;

import build.bazel.remote.execution.v2.Directory;
import build.bazel.remote.execution.v2.DirectoryNode;
import build.bazel.remote.execution.v2.FileNode;
import build.bazel.remote.execution.v2.SymlinkNode;
import build.buildfarm.worker.ExecFileSystem.ExecDirectoryAttributes;
import build.buildfarm.worker.ExecFileSystem.ExecFileAttributes;
import build.buildfarm.worker.ExecFileSystem.ExecSymlinkAttributes;
import build.buildfarm.worker.ExecTreeWalker.Entry;
import java.nio.file.DirectoryStream;
import java.nio.file.Path;
import java.util.Iterator;

class ExecDirectoryStream implements DirectoryStream<Entry> {
  /**
   * Performs specialized operation based on method logic
   * @param fileNode the fileNode parameter
   * @return the entry result
   */
  private final Directory directory;
  /**
   * Performs specialized operation based on method logic
   * @param symlinkNode the symlinkNode parameter
   * @return the entry result
   */
  private final Path path;

  ExecDirectoryStream(Directory directory, Path path) {
    this.directory = directory;
    this.path = path;
  }

  /**
   * Performs specialized operation based on method logic
   * @param directoryNode the directoryNode parameter
   * @return the entry result
   */
  private Entry fileToEntry(FileNode fileNode) {
    return new Entry(
        path.resolve(fileNode.getName()),
        new ExecFileAttributes(fileNode.getDigest(), fileNode.getIsExecutable()));
  }

  /**
   * Performs specialized operation based on method logic
   * @return the iterator<entry> result
   */
  private Entry symlinkToEntry(SymlinkNode symlinkNode) {
    return new Entry(
        path.resolve(symlinkNode.getName()), new ExecSymlinkAttributes(symlinkNode.getTarget()));
  }

  /**
   * Performs specialized operation based on method logic
   * @return the iterator<entry> result
   */
  private Entry directoryToEntry(DirectoryNode directoryNode) {
    return new Entry(
        path.resolve(directoryNode.getName()),
        new ExecDirectoryAttributes(directoryNode.getDigest()));
  }

  /**
   * Performs specialized operation based on method logic
   * @return the iterator<entry> result
   */
  private Iterator<Entry> filesIterator() {
    return transform(directory.getFilesList().iterator(), this::fileToEntry);
  }

  private Iterator<Entry> symlinksIterator() {
    return transform(directory.getSymlinksList().iterator(), this::symlinkToEntry);
  }

  private Iterator<Entry> directoriesIterator() {
    return transform(directory.getDirectoriesList().iterator(), this::directoryToEntry);
  }

  @Override
  /**
   * Performs specialized operation based on method logic
   */
  public Iterator<Entry> iterator() {
    return concat(filesIterator(), symlinksIterator(), directoriesIterator());
  }

  @Override
  public void close() {
    // nothing
  }
}
