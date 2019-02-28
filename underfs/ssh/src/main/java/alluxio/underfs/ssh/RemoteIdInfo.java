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

import alluxio.Configuration;

import com.google.common.io.Closer;
import net.schmizz.sshj.sftp.RemoteFile;
import net.schmizz.sshj.sftp.SFTPClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

import javax.annotation.concurrent.ThreadSafe;

/**
 * A class used to map remote user or group id to name.
 */
@ThreadSafe
public class RemoteIdInfo {
  private static final Logger LOG = LoggerFactory.getLogger(RemoteIdInfo.class);
  private static final int MIN_REFRESH_INTERVAL = 5 * 60 * 1000;

  private SFTPClient mSftpClient;
  private String mRemotePath;
  private long mLastRefreshTime = 0;

  // This class is designed to be thread safe, and these maps are instantiated to
  // be normal map. So the "modify" methods of maps should not be called.
  private Map<Integer, String> mId2Name = new HashMap<>();
  private Map<String, Integer> mName2Uid = new HashMap<>();

  /**
   * Construct a new {@link RemoteIdInfo}.
   *
   * @param sftpClient the client used to fetch remote file
   * @param remotePath for which path to fetch
   */
  public RemoteIdInfo(SFTPClient sftpClient, String remotePath) {
    mSftpClient = sftpClient;
    mRemotePath = remotePath;
  }

  /**
   * Guess the user name from id.
   *
   * @param id the id of the user
   * @return the user name
   */
  public String guessName(Integer id) {
    // This class is designed to be thread safe, but no lock operations such as
    // synchronized used. So statements such as mId2Name.containsKey(id) ? mId2Name.get(id) : xx
    // is not thread safe. As content of mId2Name never updated, and only modified by assignment
    // operation, so it's thread safe of assigning the value to a temporary variable.
    Map<Integer, String> map = mId2Name;
    if (!map.containsKey(id)) {
      tryRefresh();
      map = mId2Name;
    }
    return map.containsKey(id) ? map.get(id) : id.toString();
  }

  /**
   * Get the user id from its name.
   *
   * @param name the user name
   * @return the id or null if invalid name
   */
  public Integer getId(String name) {
    try {
      return getIdFromName(name, mName2Uid);
    } catch (NumberFormatException e) {
      tryRefresh();
      try {
        return getIdFromName(name, mName2Uid);
      } catch (NumberFormatException ex) {
        return null;
      }
    }
  }

  /**
   * Try to refresh when the interval between last refresh time is larger then
   * the configured threshold
   */
  public void tryRefresh() {
    long curTime = System.currentTimeMillis();
    long interval = Configuration.getMs(SshUFSPropertyKey.SSH_IDMAP_REFRESH_INTERVAL);
    if (curTime - mLastRefreshTime < interval) {
      return;
    }
    mId2Name = getIdNameMap(mRemotePath);
    mName2Uid = mId2Name.entrySet().stream().collect(
        Collectors.toMap(Map.Entry::getValue, Map.Entry::getKey));
    mLastRefreshTime = curTime;
  }

  // If member field mName2Uid used instead of second argument directly
  // this method will not be thread safe, because mName2Uid may be
  // assigned to another map after containsKey method called.
  private Integer getIdFromName(String name, Map<String, Integer> map)
      throws NumberFormatException {
    if (map.containsKey(name)) {
      return map.get(name);
    }
    return Integer.parseInt(name);
  }

  private Map<Integer, String> getIdNameMap(String path) {
    Map<Integer, String> ret = new HashMap<>();
    try (Closer closer = Closer.create()) {
      RemoteFile file = mSftpClient.open(path);
      closer.register(file);
      InputStream stream = file.new RemoteFileInputStream();
      closer.register(stream);
      BufferedReader reader = new BufferedReader(new InputStreamReader(stream));
      closer.register(reader);

      // Currently, only these two files: /etc/passwd and /etc/group is supported,
      // and the format of each line like this:
      // work:x:500:500::/home/work:/bin/bash
      // or
      // work:x:500:
      // which is split by colon, only first and third part are used
      String line;
      while ((line = reader.readLine()) != null) {
        String[] splits = line.split(":", 4);
        if (splits.length < 3) {
          continue;
        }
        ret.put(Integer.valueOf(splits[2]), splits[0]);
      }
    } catch (IOException e) {
      LOG.info("get id map file failed: " + path, e);
    }
    return ret;
  }
}
