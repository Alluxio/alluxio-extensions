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

package alluxio.underfs.dummy;

import alluxio.AlluxioURI;
import alluxio.conf.AlluxioConfiguration;
import alluxio.underfs.BaseUnderFileSystem;
import alluxio.underfs.UfsDirectoryStatus;
import alluxio.underfs.UfsFileStatus;
import alluxio.underfs.UfsStatus;
import alluxio.underfs.UnderFileSystem;
import alluxio.underfs.UnderFileSystemConfiguration;
import alluxio.underfs.local.LocalUnderFileSystem;
import alluxio.underfs.options.CreateOptions;
import alluxio.underfs.options.DeleteOptions;
import alluxio.underfs.options.FileLocationOptions;
import alluxio.underfs.options.MkdirsOptions;
import alluxio.underfs.options.OpenOptions;
import alluxio.util.io.PathUtils;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Dummy {@link UnderFileSystem} implementation for tutorial. All operations are delegated to a
 * {@link LocalUnderFileSystem}.
 */
@ThreadSafe
public class DummyUnderFileSystem extends BaseUnderFileSystem {
  public static final String DUMMY_SCHEME = "dummy://";

  private UnderFileSystem mLocalUnderFileSystem;

  /** The configuration for ufs. */
  private final UnderFileSystemConfiguration mConf;

  /**
   * Constructs a new {@link DummyUnderFileSystem}.
   *
   * @param uri the {@link AlluxioURI} for this UFS
   * @param ufsConf UFS configuration
   */
  public DummyUnderFileSystem(AlluxioURI uri, UnderFileSystemConfiguration ufsConf,
      AlluxioConfiguration alluxioConf) {
    super(uri, ufsConf, alluxioConf);

    mLocalUnderFileSystem =
        new LocalUnderFileSystem(new AlluxioURI(mapPath(uri.getPath())), ufsConf, alluxioConf);
    mConf = ufsConf;
  }

  @Override
  public String getUnderFSType() {
    return "dummy";
  }

  @Override
  public void close() throws IOException {
    mLocalUnderFileSystem.close();
  }

  @Override
  public OutputStream create(String path, CreateOptions options) throws IOException {
    return mLocalUnderFileSystem.create(mapPath(path), options);
  }

  @Override
  public boolean deleteDirectory(String path, DeleteOptions options) throws IOException {
    return mLocalUnderFileSystem.deleteDirectory(mapPath(path), options);
  }

  @Override
  public boolean deleteFile(String path) throws IOException {
    return mLocalUnderFileSystem.deleteFile(mapPath(path));
  }

  @Override
  public boolean exists(String path) throws IOException {
    return mLocalUnderFileSystem.exists(mapPath(path));
  }

  @Override
  public long getBlockSizeByte(String path) throws IOException {
    return mLocalUnderFileSystem.getBlockSizeByte(mapPath(path));
  }

  @Override
  public UfsDirectoryStatus getDirectoryStatus(String path) throws IOException {
    return mLocalUnderFileSystem.getDirectoryStatus(mapPath(path));
  }

  @Override
  public List<String> getFileLocations(String path) throws IOException {
    return mLocalUnderFileSystem.getFileLocations(mapPath(path));
  }

  @Override
  public List<String> getFileLocations(String path, FileLocationOptions options)
      throws IOException {
    return mLocalUnderFileSystem.getFileLocations(mapPath(path), options);
  }

  @Override
  public UfsFileStatus getFileStatus(String path) throws IOException {
    return mLocalUnderFileSystem.getFileStatus(mapPath(path));
  }

  @Override
  public long getSpace(String path, SpaceType type) throws IOException {
    return mLocalUnderFileSystem.getSpace(mapPath(path), type);
  }

  @Override
  public UfsStatus getStatus(String path) throws IOException {
    return mLocalUnderFileSystem.getStatus(path);
  }

  @Override
  public boolean isDirectory(String path) throws IOException {
    return mLocalUnderFileSystem.isDirectory(mapPath(path));
  }

  @Override
  public boolean isFile(String path) throws IOException {
    return mLocalUnderFileSystem.isFile(mapPath(path));
  }

  @Override
  public UfsStatus[] listStatus(String path) throws IOException {
    return mLocalUnderFileSystem.listStatus(mapPath(path));
  }

  @Override
  public boolean mkdirs(String path, MkdirsOptions options) throws IOException {
    return mLocalUnderFileSystem.mkdirs(mapPath(path), options);
  }

  @Override
  public InputStream open(String path, OpenOptions options) throws IOException {
    return mLocalUnderFileSystem.open(mapPath(path), options);
  }

  @Override
  public boolean renameDirectory(String src, String dst) throws IOException {
    return mLocalUnderFileSystem.renameDirectory(mapPath(src), mapPath(dst));
  }

  @Override
  public boolean renameFile(String src, String dst) throws IOException {
    return mLocalUnderFileSystem.renameFile(mapPath(src), mapPath(dst));
  }

  @Override
  public void setOwner(String path, String user, String group) throws IOException {
    mLocalUnderFileSystem.setOwner(mapPath(path), user, group);
  }

  @Override
  public void setMode(String path, short mode) throws IOException {
    mLocalUnderFileSystem.setMode(mapPath(path), mode);
  }

  @Override
  public void connectFromMaster(String hostname) throws IOException {
    mLocalUnderFileSystem.connectFromMaster(hostname);
  }

  @Override
  public void connectFromWorker(String hostname) throws IOException {
    mLocalUnderFileSystem.connectFromWorker(hostname);
  }

  @Override
  public boolean supportsFlush() {
    return mLocalUnderFileSystem.supportsFlush();
  }

  @Override
  public void cleanup() {}

  /**
   * Map path to ${path}${suffix} in the local ufs.
   *
   * @param path the path to strip the scheme from
   * @return the path, with the optional scheme stripped away
   */
  private String mapPath(String path) {
    if (path.startsWith(DUMMY_SCHEME)) {
      path = path.substring(DUMMY_SCHEME.length());
    }
    //final String suffix = mConf.get(DummyUnderFileSystemPropertyKey.DUMMY_UFS_SUFFIX);
    return new AlluxioURI(path).getPath();
  }
}
