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
import alluxio.Configuration;
import alluxio.underfs.UnderFileSystemConfiguration;

import net.schmizz.sshj.SSHClient;
import net.schmizz.sshj.sftp.RemoteFile;
import net.schmizz.sshj.sftp.SFTPClient;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Map;

/**
 * Unit tests for {@link SftpFactory}.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(SftpFactory.class)
public class SftpFactoryTest {
  private SSHClient mSshClientMock;
  private SFTPClient mSftpClientMock;
  private UnderFileSystemConfiguration mUfsConf;

  @Before
  public void setup() throws Exception {
    mSshClientMock = Mockito.mock(SSHClient.class);
    mSftpClientMock = Mockito.mock(SFTPClient.class, Mockito.RETURNS_DEEP_STUBS);
    mUfsConf = UnderFileSystemConfiguration.defaults();

    PowerMockito.whenNew(SSHClient.class).withAnyArguments().thenReturn(mSshClientMock);
    Mockito.when(mSshClientMock.newSFTPClient()).thenReturn(mSftpClientMock);
  }

  @Test
  public void testConnectParam() throws Exception {
    AlluxioURI uri = new AlluxioURI("pfs", "baidu.com", "/path");
    SftpFactory.createSftpClient(uri, mUfsConf);

    uri = new AlluxioURI("pfs", "baidu.com:123", "/path");
    Map<String, String> confMap = new HashMap<>();
    confMap.put(SshUFSPropertyKey.SSH_USERNAME.toString(), "user");
    confMap.put(SshUFSPropertyKey.SSH_PASSWORD.toString(), "pwd");
    mUfsConf.setUserSpecifiedConf(confMap);
    SftpFactory.createSftpClient(uri, mUfsConf);

    Mockito.verify(mSshClientMock, Mockito.times(1)).connect("baidu.com");
    Mockito.verify(mSshClientMock, Mockito.times(1)).connect("baidu.com", 123);
    Mockito.verify(mSshClientMock, Mockito.times(1)).authPassword("user", "pwd");
  }

  @Test
  public void testAutoConnectClose() throws Exception {
    SFTPClient client = SftpFactory.createSftpClient(new AlluxioURI("/path"), mUfsConf);
    // new sftp client created when connecting
    Mockito.verify(mSshClientMock, Mockito.times(1)).newSFTPClient();

    forwardIdleInterval();
    SftpFactory.scanIdle();

    // after configured idle interval, the sftp connection will be closed
    Mockito.verify(mSshClientMock, Mockito.times(1)).close();
    Mockito.verify(mSftpClientMock, Mockito.times(1)).close();

    client.chmod("/path", 1);
    // the sftp will be connected again if new interface called
    Mockito.verify(mSshClientMock, Mockito.times(2)).newSFTPClient();
  }

  public void testAutoCloseWithReference() throws Exception {
    SFTPClient client = SftpFactory.createSftpClient(new AlluxioURI("/path"), mUfsConf);

    // open a RemoteFile, the sftp client must be valid before the RemoteFile closed
    RemoteFile remoteFile = client.open("/path");
    forwardIdleInterval();
    SftpFactory.scanIdle();
    // the sftp client should be auto connected
    Mockito.verify(mSshClientMock, Mockito.times(1)).newSFTPClient();
    // but it should not be closed even current time forwarded the configured idle interval
    Mockito.verify(mSftpClientMock, Mockito.never()).close();

    remoteFile.close();
    SftpFactory.scanIdle();
    // now the sftp client should be closed because the remote file closed
    Mockito.verify(mSftpClientMock).close();
  }

  public void testCreateStream() throws Exception {
    RemoteFile remoteFileMock = Mockito.mock(RemoteFile.class);
    InputStream inputStream = SftpFactory.createInputStream(remoteFileMock, 0);
    inputStream.close();
    Mockito.verify(remoteFileMock).close();

    remoteFileMock = Mockito.mock(RemoteFile.class);
    OutputStream outputStream = SftpFactory.createOutputStream(remoteFileMock);
    outputStream.close();
    Mockito.verify(remoteFileMock).close();
  }

  private void forwardIdleInterval() {
    long curTime = System.currentTimeMillis();
    PowerMockito.mockStatic(System.class);
    long idleInterval = Configuration.getMs(SshUFSPropertyKey.SSH_MAX_IDLE_INTERVAL);
    long timestamp = curTime + 10 * idleInterval;
    PowerMockito.when(System.currentTimeMillis()).thenReturn(timestamp);
  }
}
