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
import alluxio.util.SleepUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
  private static final Logger LOG = LoggerFactory.getLogger(DummyUnderFileSystem.class);

  public static final String DUMMY_SCHEME = "dummy://";

  private UnderFileSystem mLocalUnderFileSystem;

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
        new LocalUnderFileSystem(new AlluxioURI(stripPath(uri.getPath())), ufsConf, alluxioConf);
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
    return mLocalUnderFileSystem.create(stripPath(path), options);
  }

  @Override
  public boolean deleteDirectory(String path, DeleteOptions options) throws IOException {
    return mLocalUnderFileSystem.deleteDirectory(stripPath(path), options);
  }

  @Override
  public boolean deleteFile(String path) throws IOException {
    return mLocalUnderFileSystem.deleteFile(stripPath(path));
  }

  @Override
  public boolean exists(String path) throws IOException {
    return mLocalUnderFileSystem.exists(stripPath(path));
  }

  @Override
  public long getBlockSizeByte(String path) throws IOException {
    return mLocalUnderFileSystem.getBlockSizeByte(stripPath(path));
  }

  @Override
  public UfsDirectoryStatus getDirectoryStatus(String path) throws IOException {
    return mLocalUnderFileSystem.getDirectoryStatus(stripPath(path));
  }

  @Override
  public List<String> getFileLocations(String path) throws IOException {
    return mLocalUnderFileSystem.getFileLocations(stripPath(path));
  }

  @Override
  public List<String> getFileLocations(String path, FileLocationOptions options)
      throws IOException {
    return mLocalUnderFileSystem.getFileLocations(stripPath(path), options);
  }

  @Override
  public UfsFileStatus getFileStatus(String path) throws IOException {
    return mLocalUnderFileSystem.getFileStatus(stripPath(path));
  }

  @Override
  public long getSpace(String path, SpaceType type) throws IOException {
    return mLocalUnderFileSystem.getSpace(stripPath(path), type);
  }

  @Override
  public UfsStatus getStatus(String path) throws IOException {
    return mLocalUnderFileSystem.getStatus(path);
  }

  @Override
  public boolean isDirectory(String path) throws IOException {
    return mLocalUnderFileSystem.isDirectory(stripPath(path));
  }

  @Override
  public boolean isFile(String path) throws IOException {
    return mLocalUnderFileSystem.isFile(stripPath(path));
  }

  @Override
  public UfsStatus[] listStatus(String path) throws IOException {
    return mLocalUnderFileSystem.listStatus(stripPath(path));
  }

  @Override
  public boolean mkdirs(String path, MkdirsOptions options) throws IOException {
    return mLocalUnderFileSystem.mkdirs(stripPath(path), options);
  }

  @Override
  public InputStream open(String path, OpenOptions options) throws IOException {
    return mLocalUnderFileSystem.open(stripPath(path), options);
  }

  @Override
  public boolean renameDirectory(String src, String dst) throws IOException {
    return mLocalUnderFileSystem.renameDirectory(stripPath(src), stripPath(dst));
  }

  @Override
  public boolean renameFile(String src, String dst) throws IOException {
    return mLocalUnderFileSystem.renameFile(stripPath(src), stripPath(dst));
  }

  @Override
  public void setOwner(String path, String user, String group) throws IOException {
    mLocalUnderFileSystem.setOwner(stripPath(path), user, group);
  }

  @Override
  public void setMode(String path, short mode) throws IOException {
    mLocalUnderFileSystem.setMode(stripPath(path), mode);
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
   * Sleep and strip scheme from path.
   *
   * @param path the path to strip the scheme from
   * @return the path, with the optional scheme stripped away
   */
  private String stripPath(String path) {
    LOG.debug("Sleeping for configured interval");
    SleepUtils.sleepMs(mUfsConf.getMs(DummyUnderFileSystemPropertyKey.DUMMY_UFS_SLEEP));

    if (path.startsWith(DUMMY_SCHEME)) {
      path = path.substring(DUMMY_SCHEME.length());
    }
    return new AlluxioURI(path).getPath();
  }
}
