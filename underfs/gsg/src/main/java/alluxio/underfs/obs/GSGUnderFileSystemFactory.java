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

package alluxio.underfs.gsg;

import alluxio.AlluxioURI;
import alluxio.Constants;
import alluxio.underfs.UnderFileSystem;
import alluxio.underfs.UnderFileSystemConfiguration;
import alluxio.underfs.UnderFileSystemFactory;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Factory for creating {@link GSGUnderFileSystem}. It will ensure Google credentials are present
 * before returning a client. The validity of the credentials is checked by the client.
 */
@ThreadSafe
public final class GSGUnderFileSystemFactory implements UnderFileSystemFactory {
  private static final Logger LOG = LoggerFactory.getLogger(GSGUnderFileSystemFactory.class);

  /**
   * Constructs a new {@link GSGUnderFileSystemFactory}.
   */
  public GSGUnderFileSystemFactory() {}

  @Override
  public UnderFileSystem create(String path, UnderFileSystemConfiguration conf) {
    Preconditions.checkNotNull(path, "path");

    try {
      return GSGUnderFileSystem.createInstance(new AlluxioURI(path), conf);
    } catch (IOException e) {
      LOG.error("Failed to create GSGUnderFileSystem.", e);
      throw Throwables.propagate(e);
    }
  }

  @Override
  public boolean supportsPath(String path) {
    return path != null && path.startsWith(GSGConstants.HEADER_GSG);
  }
}
