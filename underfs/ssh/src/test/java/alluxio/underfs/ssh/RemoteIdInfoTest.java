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

import net.schmizz.sshj.sftp.RemoteFile;
import net.schmizz.sshj.sftp.SFTPClient;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Matchers;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

/**
 * Unit tests for {@link RemoteIdInfo}.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(RemoteIdInfo.class)
public class RemoteIdInfoTest {

  @Mock
  private BufferedReader mReaderMock;

  private RemoteIdInfo mRemoteIdInfo;

  @Before
  public void setup() throws Exception {
    RemoteFile remoteFileMock = Mockito.mock(RemoteFile.class);
    SFTPClient sftpClient = Mockito.mock(SFTPClient.class);
    Mockito.when(sftpClient.open(Matchers.anyString())).thenReturn(remoteFileMock);
    PowerMockito
        .whenNew(InputStreamReader.class)
        .withAnyArguments()
        .thenReturn(Mockito.mock(InputStreamReader.class));

    PowerMockito.whenNew(BufferedReader.class).withAnyArguments().thenReturn(mReaderMock);

    mRemoteIdInfo = new RemoteIdInfo(
        sftpClient, Configuration.get(SshUFSPropertyKey.SSH_GROUPFILE_PATH));
  }

  @Test
  public void testGuessName() throws IOException {
    Mockito.when(mReaderMock.readLine()).thenReturn("user:x:1:1:", null);

    Assert.assertEquals("user", mRemoteIdInfo.guessName(1));
    Assert.assertEquals("2", mRemoteIdInfo.guessName(2));
  }

  @Test
  public void testGetId() throws IOException {
    Mockito.when(mReaderMock.readLine()).thenReturn("user:x:1:1:", null);

    Assert.assertEquals(Integer.valueOf(1), mRemoteIdInfo.getId("user"));
    Assert.assertEquals(Integer.valueOf(2), mRemoteIdInfo.getId("2"));
    Assert.assertNull(mRemoteIdInfo.getId("nonexist"));
  }

  @Test
  public void testRefresh() throws IOException {
    Mockito.when(mReaderMock.readLine()).thenReturn(null, "user:x:1:1:", null);

    Assert.assertNull(mRemoteIdInfo.getId("user"));

    long curTime = System.currentTimeMillis();
    PowerMockito.mockStatic(System.class);
    PowerMockito.when(System.currentTimeMillis()).thenReturn(curTime + 24 * 60 * 60 * 1000);

    Assert.assertEquals(Integer.valueOf(1), mRemoteIdInfo.getId("user"));
  }
}
