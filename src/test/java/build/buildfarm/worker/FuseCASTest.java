// Copyright 2018 The Bazel Authors. All rights reserved.
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

package build.buildfarm.worker;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;

import build.bazel.remote.execution.v2.Digest;
import build.bazel.remote.execution.v2.Directory;
import build.bazel.remote.execution.v2.FileNode;
import com.google.protobuf.ByteString;
import java.io.IOException;
import java.nio.charset.Charset;
import jnr.constants.platform.OpenFlags;
import jnr.ffi.Pointer;
import jnr.ffi.Runtime;
import jnr.ffi.Struct;
import jnr.ffi.provider.DelegatingMemoryIO;
import jnr.ffi.provider.converters.StringResultConverter;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import ru.serce.jnrfuse.ErrorCodes;
import ru.serce.jnrfuse.struct.FileStat;
import ru.serce.jnrfuse.struct.FuseFileInfo;

@RunWith(JUnit4.class)
public class FuseCASTest {
  private FuseCAS fuseCAS;

  private final ByteString content = ByteString.copyFromUtf8("Peanut Butter");

  @Before
  public void setUp() {
    fuseCAS =
        new FuseCAS(
            null,
            (blobDigest, offset) -> {
              if (blobDigest.getHash().equals("/test")) {
                return Directory.newBuilder()
                    .addFiles(
                        FileNode.newBuilder()
                            .setName("file")
                            .setDigest(
                                Digest.newBuilder()
                                    .setHash("/test/file")
                                    .setSizeBytes(content.size()))
                            .build())
                    .build()
                    .toByteString()
                    .newInput();
              } else if (blobDigest.getHash().equals("/test/file")) {
                return content.newInput();
              }
              throw new UnsupportedOperationException();
            });
  }

  private FileStat createFileStat() {
    return new FileStat(Runtime.getSystemRuntime());
  }

  @Test
  public void getattrNoEntry() {
    assertThat(fuseCAS.getattr("/does_not_exist", createFileStat()))
        .isEqualTo(-ErrorCodes.ENOENT());
  }

  @Test
  public void getattrRegular() throws IOException, InterruptedException {
    fuseCAS.createInputRoot("test", Digest.newBuilder().setHash("/test").build());
    FileStat fileStat = createFileStat();
    assertThat(fuseCAS.getattr("/test/file", fileStat)).isEqualTo(0);
    assertThat(fileStat.st_mode.longValue() & FileStat.S_IFREG).isEqualTo(FileStat.S_IFREG);
    assertThat(fileStat.st_size.longValue()).isEqualTo(content.size());
  }

  @Test
  public void createInputRootMakesDirectory() throws IOException, InterruptedException {
    fuseCAS.createInputRoot("test", Digest.newBuilder().build());
    assertThat(fuseCAS.getattr("/test", createFileStat())).isEqualTo(0);
  }

  @Test(expected = IllegalArgumentException.class)
  public void createInputRootEmptyTopdirThrows() throws IOException, InterruptedException {
    fuseCAS.createInputRoot("", Digest.newBuilder().build());
  }

  @Test(expected = IllegalArgumentException.class)
  public void createInputRootEmptyAfterSlashes() throws IOException, InterruptedException {
    fuseCAS.createInputRoot("///", Digest.newBuilder().build());
  }

  @Test
  public void createInputRootFileAsDirectoryThrows() throws IOException, InterruptedException {
    fuseCAS.createInputRoot("test", Digest.newBuilder().setHash("/test").build());
    assertThrows(
        IllegalArgumentException.class,
        () -> {
          fuseCAS.createInputRoot("test/file/subdir", Digest.newBuilder().build());
        });
  }

  @Test
  public void createInputRootEmptyComponentsIgnored() throws IOException, InterruptedException {
    fuseCAS.createInputRoot("/test/", Digest.newBuilder().setHash("/test").build());
    assertThrows(
        IllegalArgumentException.class,
        () -> {
          fuseCAS.createInputRoot("test/file/subdir", Digest.newBuilder().build());
        });
  }

  @Test
  public void destroyInputRootRemovesDirectory() throws IOException, InterruptedException {
    fuseCAS.createInputRoot("test", Digest.newBuilder().build());
    fuseCAS.destroyInputRoot("test");
    assertThat(fuseCAS.getattr("/test", createFileStat())).isEqualTo(-ErrorCodes.ENOENT());
  }

  @Test
  public void readlinkNoEntry() {
    assertThat(fuseCAS.readlink("/does_not_exist", null, 0)).isEqualTo(-ErrorCodes.ENOENT());
  }

  @Test
  public void readlinkIsNotSymlink() throws IOException, InterruptedException {
    fuseCAS.createInputRoot("test", Digest.newBuilder().build());
    assertThat(fuseCAS.readlink("/test", null, 0)).isEqualTo(-ErrorCodes.EINVAL());
  }

  @Test
  public void symlinkNoEntry() {
    assertThat(fuseCAS.symlink("foo", "/bar/baz")).isEqualTo(-ErrorCodes.ENOENT());
  }

  @Test
  public void symlinkCreatesEntry() {
    fuseCAS.symlink("foo", "/bar");
    FileStat fileStat = createFileStat();
    assertThat(fuseCAS.getattr("/bar", fileStat)).isEqualTo(0);
    assertThat(fileStat.st_mode.longValue() & FileStat.S_IFLNK).isEqualTo(FileStat.S_IFLNK);
  }

  public static final class u8 extends Struct {
    public final Unsigned8 u8 = new Unsigned8();

    public u8(Runtime runtime) {
      super(runtime);
    }
  }

  private static String stringFromPointer(Pointer buf) {
    return StringResultConverter.getInstance(Charset.defaultCharset()).fromNative(buf, null);
  }

  private static Pointer pointerFromByteString(ByteString byteString) {
    u8[] array = Struct.arrayOf(Runtime.getSystemRuntime(), u8.class, byteString.size());
    Pointer buf = ((DelegatingMemoryIO) Struct.getMemory(array[0])).getDelegatedMemoryIO();
    for (int i = 0; i < byteString.size(); i++) {
      array[i].u8.set(byteString.byteAt(i));
    }
    return buf;
  }

  @Test
  public void readlinkCopiesLinkName() {
    fuseCAS.symlink("foo", "/bar");
    u8[] array = Struct.arrayOf(Runtime.getSystemRuntime(), u8.class, 32);
    Pointer buf = ((DelegatingMemoryIO) Struct.getMemory(array[0])).getDelegatedMemoryIO();
    fuseCAS.readlink("/bar", buf, array.length);
    String value = stringFromPointer(buf);
    assertThat(value).isEqualTo("foo");
  }

  static class SystemFuseFileInfo extends FuseFileInfo {
    public SystemFuseFileInfo() {
      super(Runtime.getSystemRuntime());
    }
  }

  @Test
  public void createNoEntry() {
    //noinspection OctalInteger
    assertThat(fuseCAS.create("/foo/bar", 0644, new SystemFuseFileInfo()))
        .isEqualTo(-ErrorCodes.ENOENT());
  }

  @Test
  public void createEntryExists() {
    //noinspection OctalInteger
    assertThat(fuseCAS.create("/foo", 0644, new SystemFuseFileInfo())).isEqualTo(0);
    FileStat fileStat = createFileStat();
    assertThat(fuseCAS.getattr("/foo", fileStat)).isEqualTo(0);
    assertThat(fileStat.st_size.longValue()).isEqualTo(0);
  }

  @Test
  public void unlinkNoEntry() {
    assertThat(fuseCAS.unlink("/does_not_exist")).isEqualTo(-ErrorCodes.ENOENT());
    assertThat(fuseCAS.unlink("/does_not_exist/foo")).isEqualTo(-ErrorCodes.ENOENT());
  }

  @Test
  public void mkdirCreatesDirectory() {
    //noinspection OctalInteger
    assertThat(fuseCAS.mkdir("/foo", 0755)).isEqualTo(0);
    FileStat fileStat = createFileStat();
    assertThat(fuseCAS.getattr("/foo", fileStat)).isEqualTo(0);
    assertThat(fileStat.st_mode.longValue() & FileStat.S_IFDIR).isEqualTo(FileStat.S_IFDIR);
  }

  @Test
  public void mkdirNoEntry() {
    //noinspection OctalInteger
    assertThat(fuseCAS.mkdir("/foo/bar", 0755)).isEqualTo(-ErrorCodes.ENOENT());
  }

  @Test
  public void mkdirExists() throws IOException, InterruptedException {
    fuseCAS.createInputRoot("test", Digest.newBuilder().setHash("/test").build());
    //noinspection OctalInteger
    assertThat(fuseCAS.mkdir("/test", 0755)).isEqualTo(-ErrorCodes.EEXIST());
  }

  @Test
  public void unlinkDirectory() {
    //noinspection OctalInteger
    fuseCAS.mkdir("/foo", 0755);
    assertThat(fuseCAS.unlink("/foo")).isEqualTo(-ErrorCodes.EISDIR());
  }

  @Test
  public void unlinkRemovesFile() {
    //noinspection OctalInteger
    assertThat(fuseCAS.create("/foo", 0644, new SystemFuseFileInfo())).isEqualTo(0);
    assertThat(fuseCAS.unlink("/foo")).isEqualTo(0);
    assertThat(fuseCAS.getattr("/foo", createFileStat())).isEqualTo(-ErrorCodes.ENOENT());
  }

  @Test
  public void renameNoEntry() {
    assertThat(fuseCAS.rename("/foo", "/bar")).isEqualTo(-ErrorCodes.ENOENT());
    assertThat(fuseCAS.rename("/foo", "/does_not_exist/bar")).isEqualTo(-ErrorCodes.ENOENT());
    assertThat(fuseCAS.rename("/does_not_exist/foo", "/bar")).isEqualTo(-ErrorCodes.ENOENT());
  }

  @Test
  public void renameCreatesNewAndRemovesOld() {
    //noinspection OctalInteger
    fuseCAS.create("/foo", 0644, new SystemFuseFileInfo());
    assertThat(fuseCAS.rename("/foo", "/bar")).isEqualTo(0);
    assertThat(fuseCAS.getattr("/bar", createFileStat())).isEqualTo(0);
    assertThat(fuseCAS.getattr("/foo", createFileStat())).isEqualTo(-ErrorCodes.ENOENT());
  }

  @Test
  public void chownNoEntry() {
    assertThat(fuseCAS.chown("/foo", -1, -1)).isEqualTo(-ErrorCodes.ENOENT());
  }

  @Test
  public void chownNop() {
    //noinspection OctalInteger
    fuseCAS.create("/foo", 0644, new SystemFuseFileInfo());
    assertThat(fuseCAS.chown("/foo", -1, -1)).isEqualTo(0);
  }

  @Test
  public void chownValidId() {
    //noinspection OctalInteger
    fuseCAS.create("/foo", 0644, new SystemFuseFileInfo());
    assertThat(fuseCAS.chown("/foo", -1, 0)).isEqualTo(-ErrorCodes.EPERM());
    assertThat(fuseCAS.chown("/foo", 0, -1)).isEqualTo(-ErrorCodes.EPERM());
  }

  @Test
  public void truncateNoEntry() {
    assertThat(fuseCAS.truncate("/foo", 0)).isEqualTo(-ErrorCodes.ENOENT());
  }

  @Test
  public void truncateExtends() {
    //noinspection OctalInteger
    fuseCAS.create("/foo", 0644, new SystemFuseFileInfo());
    assertThat(fuseCAS.truncate("/foo", 1024)).isEqualTo(0);
    FileStat fileStat = createFileStat();
    assertThat(fuseCAS.getattr("/foo", fileStat)).isEqualTo(0);
    assertThat(fileStat.st_size.longValue()).isEqualTo(1024);
  }

  @Test
  public void truncateShortens() {
    //noinspection OctalInteger
    fuseCAS.create("/foo", 0644, new SystemFuseFileInfo());
    assertThat(fuseCAS.truncate("/foo", 1024)).isEqualTo(0);
    assertThat(fuseCAS.truncate("/foo", 10)).isEqualTo(0);

    FileStat fileStat = createFileStat();
    assertThat(fuseCAS.getattr("/foo", fileStat)).isEqualTo(0);
    assertThat(fileStat.st_size.longValue()).isEqualTo(10);
  }

  @Test
  public void truncateReadOnlyFile() throws IOException, InterruptedException {
    fuseCAS.createInputRoot("test", Digest.newBuilder().setHash("/test").build());
    assertThat(fuseCAS.truncate("/test/file", 1024)).isEqualTo(-ErrorCodes.EPERM());
  }

  @Test
  public void openNoEntry() {
    SystemFuseFileInfo fi = new SystemFuseFileInfo();
    fi.flags.set(0);
    assertThat(fuseCAS.open("/foo", fi)).isEqualTo(-ErrorCodes.ENOENT());

    fi.flags.set(OpenFlags.O_CREAT.intValue() | OpenFlags.O_TRUNC.intValue());
    assertThat(fuseCAS.open("/bar/baz", fi)).isEqualTo(-ErrorCodes.ENOENT());
  }

  @Test
  public void openForRead() {
    //noinspection OctalInteger
    fuseCAS.create("/foo", 0644, new SystemFuseFileInfo());
    SystemFuseFileInfo fi = new SystemFuseFileInfo();
    fi.flags.set(0);
    assertThat(fuseCAS.open("/foo", fi)).isEqualTo(0);
  }

  @Test
  public void openCreatesFile() {
    SystemFuseFileInfo fi = new SystemFuseFileInfo();
    fi.flags.set(OpenFlags.O_CREAT.intValue() | OpenFlags.O_TRUNC.intValue());
    assertThat(fuseCAS.open("/foo", fi)).isEqualTo(0);
    assertThat(fuseCAS.getattr("/foo", createFileStat())).isEqualTo(0);
  }

  @Test
  public void writeNoEntry() {
    assertThat(fuseCAS.write("/does_not_exist", null, -1, -1, new SystemFuseFileInfo()))
        .isEqualTo(-ErrorCodes.ENOENT());
  }

  @Test
  public void writeReadOnly() throws IOException, InterruptedException {
    fuseCAS.createInputRoot("test", Digest.newBuilder().setHash("/test").build());
    FuseFileInfo fi = new SystemFuseFileInfo();
    fi.flags.set(0);
    fuseCAS.open("/test/file", fi);
    assertThat(fuseCAS.write("/test/file", null, -1, -1, fi)).isEqualTo(-ErrorCodes.EPERM());
  }

  @Test
  public void writeExtendsAndOverwrites() {
    FuseFileInfo fi = new SystemFuseFileInfo();
    fi.flags.set(OpenFlags.O_CREAT.intValue() | OpenFlags.O_TRUNC.intValue());
    //noinspection OctalInteger
    fuseCAS.create("/foo", 0644, fi);

    ByteString data = ByteString.copyFromUtf8("Hello, World\n");
    Pointer buf = pointerFromByteString(data);
    assertThat(fuseCAS.write("/foo", buf, data.size(), /* offset=*/ 0, fi)).isEqualTo(data.size());
    FileStat fileStat = createFileStat();
    fuseCAS.getattr("/foo", fileStat);
    assertThat(fileStat.st_size.longValue()).isEqualTo(data.size());

    ByteString overwriteData = ByteString.copyFromUtf8("Goodbye");
    Pointer overwriteBuf = pointerFromByteString(overwriteData);
    fuseCAS.write("/foo", overwriteBuf, overwriteData.size(), /* offset=*/ 0, fi);
    fuseCAS.getattr("/foo", fileStat);
    assertThat(fileStat.st_size.longValue()).isEqualTo(data.size());
  }

  @Test
  public void readNoEntry() {
    assertThat(fuseCAS.read("/does_not_exist", null, -1, -1, new SystemFuseFileInfo()))
        .isEqualTo(-ErrorCodes.ENOENT());
  }

  @Test
  public void readDirectory() {
    //noinspection OctalInteger
    fuseCAS.mkdir("/foo", 0755);
    SystemFuseFileInfo fi = new SystemFuseFileInfo();
    fi.flags.set(OpenFlags.O_DIRECTORY.intValue());
    fuseCAS.open("/foo", fi);
    assertThat(fuseCAS.read("/foo", null, -1, -1, fi)).isEqualTo(-ErrorCodes.EISDIR());
  }

  @Test
  public void readAtEndIsEmpty() {
    FuseFileInfo fi = new SystemFuseFileInfo();
    //noinspection OctalInteger
    fuseCAS.create("/foo", 0644, fi);
    assertThat(fuseCAS.read("/foo", /* buf=*/ null, /* size=*/ 1, /* offset=*/ 0, fi)).isEqualTo(0);
  }

  @Test
  public void readWritten() {
    FuseFileInfo fi = new SystemFuseFileInfo();
    //noinspection OctalInteger
    fuseCAS.create("/foo", 0644, fi);

    ByteString data = ByteString.copyFromUtf8("Hello, World\n");
    Pointer buf = pointerFromByteString(data);
    fuseCAS.write("/foo", buf, data.size(), /* offset=*/ 0, fi);

    byte[] readData = new byte[5];
    u8[] array = Struct.arrayOf(Runtime.getSystemRuntime(), u8.class, readData.length);
    Pointer readBuf = ((DelegatingMemoryIO) Struct.getMemory(array[0])).getDelegatedMemoryIO();
    assertThat(fuseCAS.read("/foo", readBuf, /* size=*/ readData.length, /* offset=*/ 7, fi))
        .isEqualTo(readData.length);
    readBuf.get(0, readData, 0, readData.length);
    assertThat(new String(readData, 0)).isEqualTo("World");
  }

  @Test
  public void readInputRooted() throws IOException, InterruptedException {
    fuseCAS.createInputRoot("test", Digest.newBuilder().setHash("/test").build());
    byte[] data = new byte[6];
    u8[] array = Struct.arrayOf(Runtime.getSystemRuntime(), u8.class, data.length);
    Pointer buf = ((DelegatingMemoryIO) Struct.getMemory(array[0])).getDelegatedMemoryIO();
    FuseFileInfo fi = new SystemFuseFileInfo();
    fi.flags.set(0);
    assertThat(fuseCAS.open("/test/file", fi)).isEqualTo(0);
    assertThat(
            fuseCAS.read("/test/file", /* buf=*/ buf, /* size=*/ data.length, /* offset=*/ 0, fi))
        .isEqualTo(data.length);
    buf.get(0, data, 0, data.length);
    assertThat(new String(data, 0)).isEqualTo("Peanut");
  }

  @Test
  public void fallocatePunchHole() {
    assertThat(
            fuseCAS.fallocate(
                "/op_not_supp",
                /* FALLOC_FL_PUNCH_HOLE */ 2,
                /* off=*/ -1,
                /* length=*/ -1,
                new SystemFuseFileInfo()))
        .isEqualTo(-ErrorCodes.EOPNOTSUPP());
  }

  @Test
  public void fallocateDirectory() {
    fuseCAS.mkdir("/foo", 755);
    assertThat(
            fuseCAS.fallocate(
                "/foo", /* mode=*/ 0, /* off=*/ -1, /* length=*/ -1, new SystemFuseFileInfo()))
        .isEqualTo(-ErrorCodes.EISDIR());
  }

  @Test
  public void fallocateReadOnly() throws IOException, InterruptedException {
    fuseCAS.createInputRoot("test", Digest.newBuilder().setHash("/test").build());
    assertThat(
            fuseCAS.fallocate(
                "/test/file",
                /* mode=*/ 0,
                /* off=*/ -1,
                /* length=*/ -1,
                new SystemFuseFileInfo()))
        .isEqualTo(-ErrorCodes.EPERM());
  }

  @Test
  public void fallocateResize() {
    //noinspection OctalInteger
    fuseCAS.create("/foo", 0644, new SystemFuseFileInfo());
    assertThat(
            fuseCAS.fallocate(
                "/foo", /* mode=*/ 0, /* off=*/ 0, /* length=*/ 1024, new SystemFuseFileInfo()))
        .isEqualTo(0);
    FileStat fileStat = createFileStat();
    fuseCAS.getattr("/foo", fileStat);
    assertThat(fileStat.st_size.longValue()).isEqualTo(1024);
  }
}
