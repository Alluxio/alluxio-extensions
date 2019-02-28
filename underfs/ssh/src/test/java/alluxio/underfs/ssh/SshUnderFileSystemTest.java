/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the "License"). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.underfs.ssh;

import alluxio.AlluxioURI;
import alluxio.Configuration;
import alluxio.security.authorization.Mode;
import alluxio.underfs.UfsDirectoryStatus;
import alluxio.underfs.UfsFileStatus;
import alluxio.underfs.UfsStatus;
import alluxio.underfs.UnderFileSystemConfiguration;
import alluxio.underfs.options.CreateOptions;
import alluxio.underfs.options.DeleteOptions;
import alluxio.underfs.options.MkdirsOptions;
import alluxio.underfs.options.OpenOptions;

import net.schmizz.sshj.sftp.FileAttributes;
import net.schmizz.sshj.sftp.FileMode;
import net.schmizz.sshj.sftp.PathComponents;
import net.schmizz.sshj.sftp.PathHelper;
import net.schmizz.sshj.sftp.RemoteResourceInfo;
import net.schmizz.sshj.sftp.SFTPClient;
import net.schmizz.sshj.sftp.SFTPEngine;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Unit tests for {@link SshUnderFileSystem}.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({SshUnderFileSystem.class, SftpFactory.class, RemoteIdInfo.class})
public class SshUnderFileSystemTest {
  private SFTPClient mSftpClientMock;
  private RemoteIdInfo mRemoteUserIdInfoMock;
  private RemoteIdInfo mRemoteGroupIdInfoMock;
  private SshUnderFileSystem mSshUnderFileSystem;

  @Before
  public void setup() throws Exception {
    mSftpClientMock = Mockito.mock(SFTPClient.class);
    PowerMockito.mockStatic(SftpFactory.class);
    PowerMockito.when(SftpFactory.createSftpClient(Mockito.anyObject(), Mockito.anyObject()))
        .thenReturn(mSftpClientMock);

    mRemoteUserIdInfoMock = Mockito.mock(RemoteIdInfo.class);
    mRemoteGroupIdInfoMock = Mockito.mock(RemoteIdInfo.class);
    PowerMockito.whenNew(RemoteIdInfo.class)
        .withArguments(Mockito.any(),
            Mockito.eq(Configuration.get(SshUFSPropertyKey.SSH_PWDFILE_PATH)))
        .thenReturn(mRemoteUserIdInfoMock);
    PowerMockito.whenNew(RemoteIdInfo.class)
        .withArguments(Mockito.any(),
            Mockito.eq(Configuration.get(SshUFSPropertyKey.SSH_GROUPFILE_PATH)))
        .thenReturn(mRemoteGroupIdInfoMock);

    mSshUnderFileSystem = new SshUnderFileSystem(
        new AlluxioURI("ssh", "host", "/path"), UnderFileSystemConfiguration.defaults());
  }

  @Test
  public void testCreate() throws Exception {
    mSshUnderFileSystem.create("p", CreateOptions.defaults());

    Mockito.verify(mSftpClientMock).open(Mockito.eq("p"), Mockito.anyObject(), Mockito.anyObject());
    PowerMockito.verifyStatic();
    SftpFactory.createOutputStream(Mockito.anyObject());
  }

  @Test
  public void testDeleteDir() throws Exception {
    FileAttributes.Builder attrBuilder = new FileAttributes.Builder();
    // stub the existence and directory type of the path
    Mockito.when(mSftpClientMock.statExistence(Mockito.anyString()))
        .thenReturn(attrBuilder.withType(FileMode.Type.DIRECTORY).build());
    List<RemoteResourceInfo> resourceInfos = new ArrayList<>();
    resourceInfos.add(new RemoteResourceInfo(
        new PathComponents("p", "file", "/"),
        attrBuilder.withType(FileMode.Type.REGULAR).build()));
    // stub the regular file in the directory path
    Mockito.when(mSftpClientMock.ls(Mockito.eq("p"))).thenReturn(resourceInfos);

    // first try to delete without recursive
    DeleteOptions options = DeleteOptions.defaults();
    mSshUnderFileSystem.deleteDirectory("p", options);
    Mockito.verify(mSftpClientMock).rmdir(Mockito.eq("p"));
    Mockito.verify(mSftpClientMock, Mockito.never()).rm(Mockito.anyString());

    // then try to delete with recursive
    options.setRecursive(true);
    mSshUnderFileSystem.deleteDirectory("p", options);
    Mockito.verify(mSftpClientMock, Mockito.times(2)).rmdir(Mockito.eq("p"));
    Mockito.verify(mSftpClientMock).rm(Mockito.eq("p/file"));
  }

  @Test
  public void testDeleteFile() throws Exception {
    mSshUnderFileSystem.deleteFile("file");
    Mockito.verify(mSftpClientMock, Mockito.times(1)).rm(Mockito.eq("file"));
  }

  @Test
  public void testExists() throws Exception {
    Mockito.when(mSftpClientMock.statExistence(Mockito.eq("p"))).thenReturn(FileAttributes.EMPTY);
    Mockito.when(mSftpClientMock.statExistence(Mockito.eq("q"))).thenReturn(null);

    Assert.assertTrue(mSshUnderFileSystem.exists("p"));
    Assert.assertFalse(mSshUnderFileSystem.exists("q"));
  }

  @Test
  public void testGetDirectoryStatus() throws Exception {
    Mockito.when(mRemoteUserIdInfoMock.guessName(Mockito.eq(500))).thenReturn("u");
    Mockito.when(mRemoteGroupIdInfoMock.guessName(Mockito.eq(504))).thenReturn("g");
    FileAttributes.Builder attrBuilder = new FileAttributes.Builder();
    attrBuilder.withUIDGID(500, 504).withPermissions(123);
    Mockito.when(mSftpClientMock.stat(Mockito.eq("p"))).thenReturn(attrBuilder.build());

    UfsDirectoryStatus status = mSshUnderFileSystem.getDirectoryStatus("p");
    Assert.assertEquals("p", status.getName());
    Assert.assertEquals("u", status.getOwner());
    Assert.assertEquals("g", status.getGroup());
    Assert.assertEquals(123, status.getMode());
  }

  @Test
  public void testGetFileStatus() throws Exception {
    Mockito.when(mRemoteUserIdInfoMock.guessName(Mockito.eq(500))).thenReturn("u");
    Mockito.when(mRemoteGroupIdInfoMock.guessName(Mockito.eq(504))).thenReturn("g");
    FileAttributes.Builder attrBuilder = new FileAttributes.Builder();
    attrBuilder.withUIDGID(500, 504).withPermissions(123).withSize(99).withAtimeMtime(88, 77);
    Mockito.when(mSftpClientMock.stat(Mockito.eq("p"))).thenReturn(attrBuilder.build());

    UfsFileStatus status = mSshUnderFileSystem.getFileStatus("p");
    Assert.assertEquals("p", status.getName());
    Assert.assertEquals("u", status.getOwner());
    Assert.assertEquals("g", status.getGroup());
    Assert.assertEquals(123, status.getMode());
    Assert.assertEquals(99, status.getContentLength());
    Assert.assertEquals(77 * 1000l, (long) status.getLastModifiedTime());
  }

  @Test
  public void testIsDirectory() throws Exception {
    FileAttributes.Builder attrBuilder = new FileAttributes.Builder();
    Mockito.when(mSftpClientMock.statExistence(Mockito.eq("p1")))
        .thenReturn(attrBuilder.withType(FileMode.Type.DIRECTORY).build());
    Mockito.when(mSftpClientMock.statExistence(Mockito.eq("p2")))
        .thenReturn(attrBuilder.withType(FileMode.Type.REGULAR).build());
    Mockito.when(mSftpClientMock.statExistence(Mockito.eq("p3"))).thenReturn(null);

    Assert.assertTrue(mSshUnderFileSystem.isDirectory("p1"));
    Assert.assertFalse(mSshUnderFileSystem.isDirectory("p2"));
    Assert.assertFalse(mSshUnderFileSystem.isDirectory("p3"));
  }

  @Test
  public void testIsFile() throws Exception {
    FileAttributes.Builder attrBuilder = new FileAttributes.Builder();
    Mockito.when(mSftpClientMock.statExistence(Mockito.eq("p1")))
        .thenReturn(attrBuilder.withType(FileMode.Type.REGULAR).build());
    Mockito.when(mSftpClientMock.statExistence(Mockito.eq("p2")))
        .thenReturn(attrBuilder.withType(FileMode.Type.DIRECTORY).build());
    Mockito.when(mSftpClientMock.statExistence(Mockito.eq("p3"))).thenReturn(null);

    Assert.assertTrue(mSshUnderFileSystem.isFile("p1"));
    Assert.assertFalse(mSshUnderFileSystem.isFile("p2"));
    Assert.assertFalse(mSshUnderFileSystem.isFile("p3"));
  }

  @Test
  public void testListStatus() throws Exception {
    RemoteResourceInfo info = new RemoteResourceInfo(
        new PathComponents("p", "file", "/"), FileAttributes.EMPTY);
    Mockito.when(mSftpClientMock.ls(Mockito.any())).thenReturn(Arrays.asList(info));

    UfsStatus[] status = mSshUnderFileSystem.listStatus("p");
    Assert.assertEquals(1, status.length);
  }

  @Test
  public void testMkdirs() throws Exception {
    PathHelper pathHelperMock = Mockito.mock(PathHelper.class);
    Mockito.when(pathHelperMock.getComponents(Mockito.eq("a/b/c")))
        .thenReturn(new PathComponents("a/b", "c", "/"));
    Mockito.when(pathHelperMock.getComponents(Mockito.eq("a/b")))
        .thenReturn(new PathComponents("a", "b", "/"));
    Mockito.when(pathHelperMock.getComponents(Mockito.eq("a")))
        .thenReturn(new PathComponents("", "a", "/"));

    SFTPEngine engineMock = Mockito.mock(SFTPEngine.class);
    Mockito.when(engineMock.getPathHelper()).thenReturn(pathHelperMock);
    FileAttributes.Builder attrBuilder = new FileAttributes.Builder();
    attrBuilder.withType(FileMode.Type.DIRECTORY);
    Mockito.when(mSftpClientMock.statExistence(Mockito.eq("/a"))).thenReturn(attrBuilder.build());
    Mockito.when(mSftpClientMock.getSFTPEngine()).thenReturn(engineMock);
    // to throw exception when setting owners
    Mockito.doThrow(new IOException()).when(mSftpClientMock)
        .setattr(Mockito.eq("a/b"), Mockito.any());

    Mode mode = new Mode((short) 123);
    MkdirsOptions options = MkdirsOptions.defaults().setCreateParent(true).setMode(mode);
    mSshUnderFileSystem.mkdirs("a/b/c", options);

    ArgumentCaptor<FileAttributes> argument = ArgumentCaptor.forClass(FileAttributes.class);
    Mockito.verify(engineMock).makeDir(Mockito.eq("a/b"), argument.capture());
    Assert.assertEquals(123, argument.getValue().getMode().getMask());
    Mockito.verify(engineMock).makeDir(Mockito.eq("a/b/c"), Mockito.any());
    Mockito.verify(mSftpClientMock).setattr(Mockito.eq("a/b"), Mockito.any());
  }

  @Test
  public void testOpen() throws Exception {
    mSshUnderFileSystem.open("p", OpenOptions.defaults().setOffset(10));
    Mockito.verify(mSftpClientMock, Mockito.times(1)).open(Mockito.eq("p"));
    PowerMockito.verifyStatic();
    SftpFactory.createInputStream(Mockito.any(), Mockito.eq(10L));
  }

  @Test
  public void testRename() throws Exception {
    FileAttributes.Builder attrBuilder = new FileAttributes.Builder();
    attrBuilder.withType(FileMode.Type.DIRECTORY);
    Mockito.when(mSftpClientMock.statExistence(Mockito.eq("src"))).thenReturn(attrBuilder.build());
    mSshUnderFileSystem.renameDirectory("src", "dest");
    Mockito.verify(mSftpClientMock).rename(Mockito.eq("src"), Mockito.eq("dest"));
  }

  @Test
  public void testSetOwner() throws Exception {
    mSshUnderFileSystem.setOwner("p", "u", "g");
    Mockito.verify(mSftpClientMock).setattr(Mockito.eq("p"), Mockito.any());
  }

  @Test
  public void testSetMode() throws Exception {
    mSshUnderFileSystem.setMode("p", (short) 123);
    Mockito.verify(mSftpClientMock).chmod(Mockito.eq("p"), Mockito.eq(123));
  }
}
