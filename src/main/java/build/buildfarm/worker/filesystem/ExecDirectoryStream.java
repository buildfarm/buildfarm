package build.buildfarm.worker.filesystem;

import static com.google.common.collect.Iterators.concat;
import static com.google.common.collect.Iterators.transform;

import build.bazel.remote.execution.v2.Directory;
import build.bazel.remote.execution.v2.DirectoryNode;
import build.bazel.remote.execution.v2.FileNode;
import build.bazel.remote.execution.v2.SymlinkNode;
import build.buildfarm.worker.filesystem.ExecFileSystem.ExecDirectoryAttributes;
import build.buildfarm.worker.filesystem.ExecFileSystem.ExecFileAttributes;
import build.buildfarm.worker.filesystem.ExecFileSystem.ExecSymlinkAttributes;
import build.buildfarm.worker.filesystem.ExecTreeWalker.Entry;
import java.nio.file.DirectoryStream;
import java.nio.file.Path;
import java.util.Iterator;

class ExecDirectoryStream implements DirectoryStream<Entry> {
  private final Directory directory;
  private final Path path;

  ExecDirectoryStream(Directory directory, Path path) {
    this.directory = directory;
    this.path = path;
  }

  private Entry fileToEntry(FileNode fileNode) {
    return new Entry(
        path.resolve(fileNode.getName()),
        new ExecFileAttributes(fileNode.getDigest(), fileNode.getIsExecutable()));
  }

  private Entry symlinkToEntry(SymlinkNode symlinkNode) {
    return new Entry(
        path.resolve(symlinkNode.getName()), new ExecSymlinkAttributes(symlinkNode.getTarget()));
  }

  private Entry directoryToEntry(DirectoryNode directoryNode) {
    return new Entry(
        path.resolve(directoryNode.getName()),
        new ExecDirectoryAttributes(directoryNode.getDigest()));
  }

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
  public Iterator<Entry> iterator() {
    return concat(filesIterator(), symlinksIterator(), directoriesIterator());
  }

  @Override
  public void close() {
    // nothing
  }
}
