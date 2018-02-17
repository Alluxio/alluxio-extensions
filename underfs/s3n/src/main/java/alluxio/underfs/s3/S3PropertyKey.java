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

package alluxio.underfs.s3;

import alluxio.PropertyKey;

import javax.annotation.concurrent.ThreadSafe;

/**
 * S3 configuration property keys.
 */
@ThreadSafe
public final class S3PropertyKey {
  public static final PropertyKey S3N_ACCESS_KEY = new PropertyKey.Builder(Name.S3N_ACCESS_KEY).build();
  public static final PropertyKey S3N_SECRET_KEY = new PropertyKey.Builder(Name.S3N_SECRET_KEY).build();

  public static final PropertyKey UNDERFS_S3_ENDPOINT_HTTP_PORT =
      new PropertyKey.Builder(Name.UNDERFS_S3_ENDPOINT_HTTP_PORT)
          .build();
  public static final PropertyKey UNDERFS_S3_ENDPOINT_HTTPS_PORT =
      new PropertyKey.Builder(Name.UNDERFS_S3_ENDPOINT_HTTPS_PORT)
          .build();
  public static final PropertyKey UNDERFS_S3_PROXY_HTTPS_ONLY =
      new PropertyKey.Builder(Name.UNDERFS_S3_PROXY_HTTPS_ONLY)
          .setDefaultValue(true)
          .setDescription("If using a proxy to communicate with S3, determine whether to talk "
              + "to the proxy using https.")
          .build();

  @ThreadSafe
  public static final class Name {
    public static final String S3N_ACCESS_KEY = "fs.s3n.awsAccessKeyId";
    public static final String S3N_SECRET_KEY = "fs.s3n.awsSecretAccessKey";

    public static final String UNDERFS_S3_ENDPOINT_HTTPS_PORT =
        "alluxio.underfs.s3.endpoint.https.port";
    public static final String UNDERFS_S3_ENDPOINT_HTTP_PORT =
        "alluxio.underfs.s3.endpoint.http.port";
    public static final String UNDERFS_S3_PROXY_HTTPS_ONLY = "alluxio.underfs.s3.proxy.https.only";
  }
}
