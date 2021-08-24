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

package alluxio.underfs.obs;

import alluxio.AlluxioURI;
import alluxio.ConfigurationTestUtils;
import alluxio.underfs.UnderFileSystemConfiguration;
import alluxio.underfs.options.DeleteOptions;

import com.obs.services.ObsClient;
import com.obs.services.exception.ObsException;
import com.obs.services.model.ListObjectsRequest;
import com.obs.services.model.ObjectMetadata;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Matchers;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.Date;

/**
 * Unit tests for the {@link OBSUnderFileSystem}.
 */
public class OBSUnderFileSystemTest {

  private OBSUnderFileSystem mOBSUnderFileSystem;
  private ObsClient mClient;

  private static final String PATH = "path";
  private static final String SRC = "src";
  private static final String DST = "dst";

  private static final String BUCKET_NAME = "bucket";
  private static final String BUCKET_TYPE = "obs";

  /**
   * Set up.
   */
  @Before
  public void before() throws InterruptedException, ObsException {
    mClient = Mockito.mock(ObsClient.class);
    mOBSUnderFileSystem = new OBSUnderFileSystem(new AlluxioURI(""), mClient, BUCKET_NAME, BUCKET_TYPE,
        UnderFileSystemConfiguration.defaults(ConfigurationTestUtils.defaults()));
  }

  /**
   * Test case for {@link OBSUnderFileSystem#deleteDirectory(String, DeleteOptions)}.
   */
  @Test
  public void deleteNonRecursiveOnServiceException() throws IOException {
    Mockito.when(mClient.listObjects(Matchers.any(ListObjectsRequest.class)))
        .thenThrow(ObsException.class);

    boolean result = mOBSUnderFileSystem.deleteDirectory(PATH,
        DeleteOptions.defaults().setRecursive(false));
    Assert.assertFalse(result);
  }

  /**
   * Test case for {@link OBSUnderFileSystem#deleteDirectory(String, DeleteOptions)}.
   */
  @Test
  public void deleteRecursiveOnServiceException() throws IOException {
    Mockito.when(mClient.listObjects(Matchers.any(ListObjectsRequest.class)))
        .thenThrow(ObsException.class);

    boolean result = mOBSUnderFileSystem.deleteDirectory(PATH,
        DeleteOptions.defaults().setRecursive(true));
    Assert.assertFalse(result);
  }

  /**
   * Test case for {@link OBSUnderFileSystem#renameFile(String, String)}.
   */
  @Test
  public void renameOnServiceException() throws IOException {
    Mockito.when(mClient.listObjects(Matchers.any(ListObjectsRequest.class)))
        .thenThrow(ObsException.class);

    boolean result = mOBSUnderFileSystem.renameFile(SRC, DST);
    Assert.assertFalse(result);
  }

  @Test
  public void judgeDirectoryInBucket() {
    ObjectMetadata fileMeta = new ObjectMetadata();
    fileMeta.setLastModified(new Date());
    fileMeta.getMetadata().put("mode", 33152);
    fileMeta.setContentLength(10L);
    ObjectMetadata dirMeta = new ObjectMetadata();
    dirMeta.setLastModified(new Date());
    dirMeta.getMetadata().put("mode", 16877);
    dirMeta.setContentLength(0L);
    Mockito.when(mClient.getObjectMetadata(BUCKET_NAME, "file1"))
            .thenReturn(fileMeta);
    Mockito.when(mClient.getObjectMetadata(BUCKET_NAME, "dir1"))
            .thenReturn(dirMeta);

    // pfs bucket
    mOBSUnderFileSystem = new OBSUnderFileSystem(new AlluxioURI(""), mClient, BUCKET_NAME, "pfs",
            UnderFileSystemConfiguration.defaults(ConfigurationTestUtils.defaults()));
    Assert.assertNotNull(mOBSUnderFileSystem.getObjectStatus("file1"));
    Assert.assertNull(mOBSUnderFileSystem.getObjectStatus("dir1"));

    // obs bucket
    mOBSUnderFileSystem = new OBSUnderFileSystem(new AlluxioURI(""), mClient, BUCKET_NAME, "obs",
            UnderFileSystemConfiguration.defaults(ConfigurationTestUtils.defaults()));
    Mockito.when(mClient.getObjectMetadata(BUCKET_NAME, "dir1"))
            .thenReturn(null);
    Assert.assertNotNull(mOBSUnderFileSystem.getObjectStatus("file1"));
    Assert.assertNull(mOBSUnderFileSystem.getObjectStatus("dir1"));
  }
}
