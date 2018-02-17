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

package alluxio.underfs.glusterfs;

import alluxio.PropertyKey;

import javax.annotation.concurrent.ThreadSafe;

/**
 * GlusterFS configuration property keys.
 */
@ThreadSafe
public final class GlusterFSPropertyKey {
  public static final PropertyKey UNDERFS_GLUSTERFS_IMPL =
      new PropertyKey.Builder(Name.UNDERFS_GLUSTERFS_IMPL)
          .setDefaultValue("org.apache.hadoop.fs.glusterfs.GlusterFileSystem")
          .setDescription("Glusterfs hook with hadoop.")
          .build();
  public static final PropertyKey UNDERFS_GLUSTERFS_MOUNTS =
      new PropertyKey.Builder(Name.UNDERFS_GLUSTERFS_MOUNTS).build();
  public static final PropertyKey UNDERFS_GLUSTERFS_MR_DIR =
      new PropertyKey.Builder(Name.UNDERFS_GLUSTERFS_MR_DIR)
          .setDefaultValue("glusterfs:///mapred/system")
          .setDescription("Optionally, specify subdirectory under GlusterFS for intermediary "
              + "MapReduce data.")
          .build();
  public static final PropertyKey UNDERFS_GLUSTERFS_VOLUMES =
      new PropertyKey.Builder(Name.UNDERFS_GLUSTERFS_VOLUMES).build();

  @ThreadSafe
  public static final class Name {
    public static final String UNDERFS_GLUSTERFS_IMPL = "alluxio.underfs.glusterfs.impl";
    public static final String UNDERFS_GLUSTERFS_MOUNTS = "alluxio.underfs.glusterfs.mounts";
    public static final String UNDERFS_GLUSTERFS_MR_DIR =
        "alluxio.underfs.glusterfs.mapred.system.dir";
    public static final String UNDERFS_GLUSTERFS_VOLUMES = "alluxio.underfs.glusterfs.volumes";
  }
}
