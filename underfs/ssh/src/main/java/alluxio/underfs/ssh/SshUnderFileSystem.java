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
import alluxio.PropertyKey;
import alluxio.exception.ExceptionMessage;
import alluxio.underfs.AtomicFileOutputStream;
import alluxio.underfs.AtomicFileOutputStreamCallback;
import alluxio.underfs.BaseUnderFileSystem;
import alluxio.underfs.UfsDirectoryStatus;
import alluxio.underfs.UfsFileStatus;
import alluxio.underfs.UfsStatus;
import alluxio.underfs.UnderFileSystem;
import alluxio.underfs.UnderFileSystemConfiguration;
import alluxio.underfs.options.CreateOptions;
import alluxio.underfs.options.DeleteOptions;
import alluxio.underfs.options.FileLocationOptions;
import alluxio.underfs.options.MkdirsOptions;
import alluxio.underfs.options.OpenOptions;
import alluxio.util.UnderFileSystemUtils;

import net.schmizz.sshj.sftp.FileAttributes;
import net.schmizz.sshj.sftp.FileMode;
import net.schmizz.sshj.sftp.OpenMode;
import net.schmizz.sshj.sftp.PathComponents;
import net.schmizz.sshj.sftp.RemoteFile;
import net.schmizz.sshj.sftp.RemoteResourceInfo;
import net.schmizz.sshj.sftp.Response;
import net.schmizz.sshj.sftp.SFTPClient;
import net.schmizz.sshj.sftp.SFTPEngine;
import net.schmizz.sshj.sftp.SFTPException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.Deque;
import java.util.EnumSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * SSH FS {@link UnderFileSystem} implementation based on the jsch-nio library.
 */
public class SshUnderFileSystem extends BaseUnderFileSystem
    implements AtomicFileOutputStreamCallback{
  private static final Logger LOG = LoggerFactory.getLogger(SshUnderFileSystem.class);

  public static final String SCHEME = "ssh://";

  private SFTPClient mSftpClient;
  private RemoteIdInfo mRemoteUserIdInfo;
  private RemoteIdInfo mRemoteGroupIdInfo;
  private String mHost;

  /**
   * Constructs a new {@link SshUnderFileSystem}.
   *
   * @param uri the {@link AlluxioURI} for this UFS
   * @param ufsConf UFS configuration
   */
  public SshUnderFileSystem(AlluxioURI uri, UnderFileSystemConfiguration ufsConf) {
    super(uri, ufsConf);
    mHost = uri.getHost();
    try {
      mSftpClient = SftpFactory.createSftpClient(uri, ufsConf);
    } catch (IOException e) {
      throw new RuntimeException("SSH login failed", e);
    }
    mRemoteUserIdInfo = new RemoteIdInfo(
        mSftpClient, Configuration.get(SshUFSPropertyKey.SSH_PWDFILE_PATH));
    mRemoteGroupIdInfo = new RemoteIdInfo(
        mSftpClient, Configuration.get(SshUFSPropertyKey.SSH_GROUPFILE_PATH));
  }

  @Override
  public String getUnderFSType() {
    return "ssh";
  }

  @Override
  public void close() throws IOException {
  }

  @Override
  public OutputStream create(String path, CreateOptions options) throws IOException {
    path = getRemotePath(path);
    if (!options.isEnsureAtomic()) {
      return createDirect(path, options);
    }
    return new AtomicFileOutputStream(path, this, options);
  }

  @Override
  public OutputStream createDirect(String path, CreateOptions options) throws IOException {
    path = getRemotePath(path);
    if (options.getCreateParent()) {
      File parent = new File(path).getParentFile();
      MkdirsOptions mkdirOptions = MkdirsOptions.defaults()
          .setCreateParent(true)
          .setOwner(options.getOwner())
          .setGroup(options.getGroup())
          .setMode(MkdirsOptions.defaults().getMode());
      if (parent != null && !mkdirs(parent.getPath(), mkdirOptions)) {
        throw new IOException(ExceptionMessage.PARENT_CREATION_FAILED.getMessage(path));
      }
    }
    Set<OpenMode> modes = EnumSet.of(OpenMode.WRITE, OpenMode.CREAT, OpenMode.TRUNC);
    FileAttributes.Builder builder = new FileAttributes.Builder();
    builder.withPermissions(options.getMode().toShort());
    Integer uid = mRemoteUserIdInfo.getId(options.getOwner());
    Integer gid = mRemoteGroupIdInfo.getId(options.getGroup());
    if (null != uid && null != gid) {
      LOG.info("will create stream with default user group");
      builder.withUIDGID(uid, gid);
    }
    RemoteFile remoteFile = mSftpClient.open(path, modes, builder.build());
    return SftpFactory.createOutputStream(remoteFile);
  }

  @Override
  public boolean deleteDirectory(String path, DeleteOptions options) throws IOException {
    path = getRemotePath(path);
    if (!isDirectory(path)) {
      return false;
    }
    if (!options.isRecursive()) {
      mSftpClient.rmdir(path);
    } else {
      deleteDirectoryRecursive(path);
    }
    return true;
  }

  @Override
  public boolean deleteFile(String path) throws IOException {
    path = getRemotePath(path);
    mSftpClient.rm(path);
    return true;
  }

  @Override
  public boolean exists(String path) throws IOException {
    path = getRemotePath(path);
    return null != mSftpClient.statExistence(path);
  }

  @Override
  public long getBlockSizeByte(String path) throws IOException {
    return Configuration.getBytes(PropertyKey.USER_BLOCK_SIZE_BYTES_DEFAULT);
  }

  @Override
  public UfsDirectoryStatus getDirectoryStatus(String path) throws IOException {
    path = getRemotePath(path);
    FileAttributes attributes = mSftpClient.stat(path);
    return generateDirectoryStatus(path, attributes);
  }

  @Override
  public List<String> getFileLocations(String path) throws IOException {
    return Arrays.asList(mHost);
  }

  @Override
  public List<String> getFileLocations(String path, FileLocationOptions options)
      throws IOException {
    return getFileLocations(path);
  }

  @Override
  public UfsFileStatus getFileStatus(String path) throws IOException {
    path = getRemotePath(path);
    FileAttributes attributes = mSftpClient.stat(path);
    return generateFileStatus(path, attributes);
  }

  @Override
  public long getSpace(String path, SpaceType type) throws IOException {
    // we do not support getting space of this fs
    return -1;
  }

  @Override
  public UfsStatus getStatus(String path) throws IOException {
    if (isFile(path)) {
      return getFileStatus(path);
    }
    return getDirectoryStatus(path);
  }

  @Override
  public boolean isDirectory(String path) throws IOException {
    path = getRemotePath(path);
    FileAttributes attributes = mSftpClient.statExistence(path);
    return null != attributes && FileMode.Type.DIRECTORY.equals(attributes.getType());
  }

  @Override
  public boolean isFile(String path) throws IOException {
    path = getRemotePath(path);
    FileAttributes attributes = mSftpClient.statExistence(path);
    return null != attributes && FileMode.Type.REGULAR.equals(attributes.getType());
  }

  @Override
  public UfsStatus[] listStatus(String path) throws IOException {
    path = getRemotePath(path);
    List<RemoteResourceInfo> resourceInfos = null;
    try {
      resourceInfos = mSftpClient.ls(path);
    } catch (SFTPException e) {
      if (e.getStatusCode() != Response.StatusCode.NO_SUCH_FILE) {
        throw e;
      }
    }
    if (null == resourceInfos || (
        resourceInfos.size() == 1 && resourceInfos.get(0).getPath().equals(path))) {
      return null;
    }

    int i = 0;
    UfsStatus[] ufsStatus = new UfsStatus[resourceInfos.size()];
    for (RemoteResourceInfo info : resourceInfos) {
      if (!info.isDirectory()) {
        ufsStatus[i++] = generateFileStatus(info.getName(), info.getAttributes());
      } else {
        ufsStatus[i++] = generateDirectoryStatus(info.getName(), info.getAttributes());
      }
    }
    return ufsStatus;
  }

  @Override
  public boolean mkdirs(String path, MkdirsOptions options) throws IOException {
    path = getRemotePath(path);
    SFTPEngine engine = mSftpClient.getSFTPEngine();
    final Deque<String> dirsToMake = new LinkedList<>();
    // generate the directories to make
    if (!options.getCreateParent()) {
      dirsToMake.push(path);
    } else {
      for (PathComponents current = engine.getPathHelper().getComponents(path);;
           current = engine.getPathHelper().getComponents(current.getParent())) {
        final FileAttributes attrs = mSftpClient.statExistence(current.getPath());
        if (attrs == null) {
          dirsToMake.push(current.getPath());
        } else if (attrs.getType() != FileMode.Type.DIRECTORY) {
          LOG.warn("Failed to make path who goes through a file: " + current.getPath());
          return false;
        } else {
          break;
        }
      }
    }

    // really make operations
    FileAttributes.Builder builder = new FileAttributes.Builder();
    FileAttributes modeAttributes = builder.withPermissions(options.getMode().toShort()).build();
    while (!dirsToMake.isEmpty()) {
      path = dirsToMake.pop();
      try {
        engine.makeDir(path, modeAttributes);
      } catch (IOException e) {
        LOG.info("failed to mkdir {} by ssh", path);
        return false;
      }
      try {
        setOwner(path, options.getOwner(), options.getGroup());
      } catch (IOException e) {
        LOG.warn("Failed to update the ufs dir ownership, default values will be used: {}",
            e.getMessage());
      }
    }
    return true;
  }

  @Override
  public InputStream open(String path, OpenOptions options) throws IOException {
    path = getRemotePath(path);
    RemoteFile remoteFile = mSftpClient.open(path);
    return SftpFactory.createInputStream(remoteFile, options.getOffset());
  }

  @Override
  public boolean renameDirectory(String src, String dst) throws IOException {
    if (!isDirectory(src)) {
      LOG.warn("Unable to rename {} to {} because source does not exist or is a file.", src, dst);
      return false;
    }
    rename(src, dst);
    return true;
  }

  @Override
  public boolean renameFile(String src, String dst) throws IOException {
    if (!isFile(src)) {
      LOG.warn("Unable to rename {} to {} because source does not exist or is a file.", src, dst);
      return false;
    }
    rename(src, dst);
    return true;
  }

  @Override
  public void setOwner(String path, String owner, String group) throws IOException {
    path = getRemotePath(path);
    Integer uid = mRemoteUserIdInfo.getId(owner);
    Integer gid = mRemoteGroupIdInfo.getId(group);
    if (null == uid || null == gid) {
      throw new IOException("invalid user or group");
    }
    FileAttributes.Builder builder = new FileAttributes.Builder();
    mSftpClient.setattr(path, builder.withUIDGID(uid, gid).build());
  }

  @Override
  public void setMode(String path, short mode) throws IOException {
    path = getRemotePath(path);
    mSftpClient.chmod(path, mode);
  }

  @Override
  public void connectFromMaster(String hostname) throws IOException {
  }

  @Override
  public void connectFromWorker(String hostname) throws IOException {
  }

  @Override
  public boolean supportsFlush() {
    return true;
  }

  private String getRemotePath(String path) {
    AlluxioURI uri = new AlluxioURI(path);
    return uri.getPath();
  }

  private void deleteDirectoryRecursive(String path) throws IOException {
    List<RemoteResourceInfo> resourceInfos = mSftpClient.ls(path);
    for (RemoteResourceInfo info : resourceInfos) {
      if (info.isDirectory()) {
        deleteDirectoryRecursive(info.getPath());
      } else {
        mSftpClient.rm(info.getPath());
      }
    }
    mSftpClient.rmdir(path);
  }

  private void rename(String src, String dst) throws IOException {
    String srcPath = getRemotePath(src);
    String dstPath = getRemotePath(dst);
    mSftpClient.rename(srcPath, dstPath);
  }

  private UfsFileStatus generateFileStatus(String path, FileAttributes attributes) {
    long modifyTime = TimeUnit.MILLISECONDS.convert(attributes.getMtime(), TimeUnit.SECONDS);
    long length = attributes.getSize();
    String contentHash = UnderFileSystemUtils.approximateContentHash(length, modifyTime);
    return new UfsFileStatus(
        path,
        contentHash,
        length,
        modifyTime,
        mRemoteUserIdInfo.guessName(attributes.getUID()),
        mRemoteGroupIdInfo.guessName(attributes.getGID()),
        (short) attributes.getMode().getMask());
  }

  private UfsDirectoryStatus generateDirectoryStatus(String path, FileAttributes attributes) {
    return new UfsDirectoryStatus(
        path,
        mRemoteUserIdInfo.guessName(attributes.getUID()),
        mRemoteGroupIdInfo.guessName(attributes.getGID()),
        (short) attributes.getMode().getMask());
  }
}
