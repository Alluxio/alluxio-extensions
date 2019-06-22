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

import alluxio.util.CommonUtils;
import alluxio.util.io.PathUtils;

import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * A stream for writing a file into GSG. The data will be persisted to a temporary directory on the
 * local disk and copied as a complete file when the {@link #close()} method is called.
 */
@NotThreadSafe
public final class GSGOutputStream extends OutputStream {
  private static final Logger LOG = LoggerFactory.getLogger(GSGOutputStream.class);

  /** Bucket name of the Alluxio GSG bucket. */
  private final String mBucketName;

  /** Key of the file when it is uploaded to GSG. */
  private final String mKey;

  /** The local file that will be uploaded when the stream is closed. */
  private final File mFile;

  /** The Google cloud storage client. */
  private final Storage mClient;

  /** The output stream to a local file where the file will be buffered until closed. */
  private OutputStream mLocalOutputStream;

  /** Flag to indicate this stream has been closed, to ensure close is only done once. */
  private AtomicBoolean mClosed = new AtomicBoolean(false);

  /**
   * Constructs a new stream for writing a file.
   *
   * @param bucketName the name of the bucket
   * @param key the key of the file
   * @param client the JetS3t client
   */
  public GSGOutputStream(String bucketName, String key, Storage client) throws IOException {
    Preconditions.checkArgument(bucketName != null && !bucketName.isEmpty(), "Bucket name must "
        + "not be null or empty.");
    mBucketName = bucketName;
    mKey = key;
    mClient = client;
    mFile = new File(PathUtils.concatPath(CommonUtils.getTmpDir(), UUID.randomUUID()));
    mLocalOutputStream = new BufferedOutputStream(new FileOutputStream(mFile));
  }

  @Override
  public void write(int b) throws IOException {
    mLocalOutputStream.write(b);
  }

  @Override
  public void write(byte[] b) throws IOException {
    mLocalOutputStream.write(b, 0, b.length);
  }

  @Override
  public void write(byte[] b, int off, int len) throws IOException {
    mLocalOutputStream.write(b, off, len);
  }

  @Override
  public void flush() throws IOException {
    mLocalOutputStream.flush();
  }

  @Override
  public void close() throws IOException {
    if (mClosed.getAndSet(true)) {
      return;
    }
    mLocalOutputStream.close();
    try {
      BlobId id = BlobId.of(mBucketName, mKey);
      BlobInfo info = BlobInfo.newBuilder(id).build();
      Blob blob = mClient.create(info, Files.readAllBytes(mFile.toPath()));
      if (blob == null) {
        throw new IOException(String.format("Failed to upload file %s to %s", mKey, mBucketName));
      }
    } catch (Exception e) {
      LOG.error("Failed to upload {} to {}: {}", mKey, mBucketName, e.toString());
      throw new IOException(e);
    } finally {
      if (!mFile.delete()) {
        LOG.error("Failed to delete temporary file @ {}", mFile.getPath());
      }
    }
  }
}
