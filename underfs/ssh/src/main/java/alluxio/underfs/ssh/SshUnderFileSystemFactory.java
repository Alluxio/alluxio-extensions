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
import alluxio.underfs.UnderFileSystem;
import alluxio.underfs.UnderFileSystemConfiguration;
import alluxio.underfs.UnderFileSystemFactory;

import com.google.common.base.Preconditions;

import javax.annotation.Nullable;

/**
 * Factory for creating {@link SshUnderFileSystem}.
 */
public class SshUnderFileSystemFactory implements UnderFileSystemFactory {

  /**
   * Constructs a new {@link SshUnderFileSystemFactory}.
   */
  public SshUnderFileSystemFactory() {}

  @Override
  public UnderFileSystem create(String path, @Nullable UnderFileSystemConfiguration conf) {
    Preconditions.checkNotNull(path, "path may not be null");
    return new SshUnderFileSystem(new AlluxioURI(path), conf);
  }

  @Override
  public boolean supportsPath(String path) {
    return path != null && path.startsWith(SshUnderFileSystem.SCHEME);
  }
}
