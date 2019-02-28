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

import alluxio.PropertyKey;

import javax.annotation.concurrent.ThreadSafe;

/**
 * SSH under file system property keys.
 */
@ThreadSafe
public class SshUFSPropertyKey {
  public static final PropertyKey SSH_USERNAME =
      new PropertyKey.Builder(Name.SSH_USERNAME)
          .setDescription("User name when login with ssh.")
          .build();
  public static final PropertyKey SSH_PASSWORD =
      new PropertyKey.Builder(Name.SSH_PASSWORD)
          .setDescription("Password when login with ssh.")
          .build();
  public static final PropertyKey SSH_PWDFILE_PATH =
      new PropertyKey.Builder(Name.SSH_PWDFILE_PATH)
          .setDefaultValue("/etc/passwd")
          .setDescription("Password file used when mapping user id to name.")
          .build();
  public static final PropertyKey SSH_GROUPFILE_PATH =
      new PropertyKey.Builder(Name.SSH_GROUPFILE_PATH)
          .setDefaultValue("/etc/group")
          .setDescription("Group file used when mapping user id to name.")
          .build();
  public static final PropertyKey SSH_IDMAP_REFRESH_INTERVAL =
      new PropertyKey.Builder(Name.SSH_IDMAP_REFRESH_INTERVAL)
          .setDefaultValue("5min")
          .setDescription("Group file used when mapping user id to name.")
          .build();
  public static final PropertyKey SSH_MAX_IDLE_INTERVAL =
      new PropertyKey.Builder(Name.SSH_MAX_IDLE_INTERVAL)
          .setDefaultValue("30min")
          .setDescription("Sftp connection will be closed after idled for the interval, " +
              "must be larger than 1 min")
          .build();

  @ThreadSafe
  public static final class Name {
    public static final String SSH_USERNAME = "ssh.username";
    public static final String SSH_PASSWORD = "ssh.password";
    public static final String SSH_PWDFILE_PATH = "ssh.pwdfile.path";
    public static final String SSH_GROUPFILE_PATH = "ssh.groupfile.path";
    public static final String SSH_IDMAP_REFRESH_INTERVAL = "ssh.idmap.refresh.interval";
    public static final String SSH_MAX_IDLE_INTERVAL = "ssh.max.idle.interval";
  }
}
