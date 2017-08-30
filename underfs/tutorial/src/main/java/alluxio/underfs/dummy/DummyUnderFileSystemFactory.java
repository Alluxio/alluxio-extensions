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
import alluxio.underfs.UnderFileSystem;
import alluxio.underfs.UnderFileSystemConfiguration;
import alluxio.underfs.UnderFileSystemFactory;

import com.google.common.base.Preconditions;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Factory for creating {@link DummyUnderFileSystem}.
 */
@ThreadSafe
public class DummyUnderFileSystemFactory implements UnderFileSystemFactory {

  /**
   * Constructs a new {@link DummyUnderFileSystemFactory}.
   */
  public DummyUnderFileSystemFactory() {}

  @Override
  public UnderFileSystem create(String path, UnderFileSystemConfiguration conf) {
    Preconditions.checkArgument(path != null, "path may not be null");
    return new DummyUnderFileSystem(new AlluxioURI(path), conf);
  }

  @Override
  public boolean supportsPath(String path) {
    if (path == null) {
      return false;
    }
    return path.startsWith(DummyUnderFileSystem.DUMMY_SCHEME);
  }
}