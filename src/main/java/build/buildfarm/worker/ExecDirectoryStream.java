package build.buildfarm.worker;

import static com.google.common.collect.Iterators.concat;
import static com.google.common.collect.Iterators.transform;

import build.bazel.remote.execution.v2.DigestFunction;
import build.bazel.remote.execution.v2.Directory;
import build.bazel.remote.execution.v2.DirectoryNode;
import build.bazel.remote.execution.v2.FileNode;
import build.bazel.remote.execution.v2.SymlinkNode;
import build.buildfarm.common.DigestUtil;
import build.buildfarm.worker.ExecFileSystem.ExecDirectoryAttributes;
import build.buildfarm.worker.ExecFileSystem.ExecFileAttributes;
import build.buildfarm.worker.ExecFileSystem.ExecSymlinkAttributes;
import build.buildfarm.worker.ExecTreeWalker.Entry;
import java.nio.file.DirectoryStream;
import java.nio.file.Path;
import java.util.Iterator;

class ExecDirectoryStream implements DirectoryStream<Entry> {
  private final Directory directory;
  private final Path path;
  private final DigestFunction.Value digestFunction;

  ExecDirectoryStream(Directory directory, Path path, DigestFunction.Value digestFunction) {
    this.directory = directory;
    this.path = path;
    this.digestFunction = digestFunction;
  }

  private Entry fileToEntry(FileNode fileNode) {
    return new Entry(
        path.resolve(fileNode.getName()),
        new ExecFileAttributes(
            DigestUtil.fromDigest(fileNode.getDigest(), digestFunction),
            fileNode.getIsExecutable()));
  }

  private Entry symlinkToEntry(SymlinkNode symlinkNode) {
    return new Entry(
        path.resolve(symlinkNode.getName()), new ExecSymlinkAttributes(symlinkNode.getTarget()));
  }

  private Entry directoryToEntry(DirectoryNode directoryNode) {
    return new Entry(
        path.resolve(directoryNode.getName()),
        new ExecDirectoryAttributes(
            DigestUtil.fromDigest(directoryNode.getDigest(), digestFunction)));
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
