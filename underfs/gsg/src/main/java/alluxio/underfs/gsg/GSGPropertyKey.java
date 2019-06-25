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

import alluxio.PropertyKey;

import javax.annotation.concurrent.ThreadSafe;

/**
 * GSG configuration property keys.
 */
@ThreadSafe
public final class GSGPropertyKey {
  public static final PropertyKey GSG_CREDENTIAL_PATH = new PropertyKey.Builder(Name.GSG_CREDENTIAL_PATH)
      .setDescription("The json file path of Google application credentials.")
      .build();

  public static final PropertyKey GSG_DEFAULT_MODE = new PropertyKey.Builder(Name.GSG_DEFAULT_MODE)
      .setDefaultValue("0700")
      .setDescription("Mode (in octal notation) for GCG objects.")
      .build();

  @ThreadSafe
  public static final class Name {
    public static final String GSG_CREDENTIAL_PATH = "fs.gsg.credential.path";
    public static final String GSG_DEFAULT_MODE = "fs.gsg.default.mode";
  }
}
