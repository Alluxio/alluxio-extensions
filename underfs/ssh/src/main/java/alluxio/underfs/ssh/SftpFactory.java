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
import alluxio.collections.ConcurrentHashSet;
import alluxio.underfs.UnderFileSystemConfiguration;
import alluxio.util.ThreadFactoryUtils;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.io.Closer;
import net.schmizz.sshj.SSHClient;
import net.schmizz.sshj.sftp.RemoteFile;
import net.schmizz.sshj.sftp.SFTPClient;
import net.schmizz.sshj.sftp.SFTPEngine;
import net.schmizz.sshj.transport.verification.PromiscuousVerifier;
import net.sf.cglib.proxy.Enhancer;
import net.sf.cglib.proxy.MethodInterceptor;
import net.sf.cglib.proxy.MethodProxy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.Method;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * The factory used to generate sftp client instance.
 */
public class SftpFactory {
  private static final Logger LOG = LoggerFactory.getLogger(SftpFactory.class);
  private static final String CLOSE_METHOD = "close";
  private static final String SSH_GUEST_USER = "guest";
  private static final int STREAM_CACHE_RW_NUM = 8;
  private static final long MAX_IDLE_INTERVAL
      = Configuration.getMs(SshUFSPropertyKey.SSH_MAX_IDLE_INTERVAL);

  private static Set<SftpContainer> sLiveContainers = new ConcurrentHashSet<>();
  private static ScheduledExecutorService sExecutor = Executors.newSingleThreadScheduledExecutor(
      ThreadFactoryUtils.build("SftpFactory-close-idle-sftp-%d", true));

  /**
   * Create a sftp client.
   * The returned instance is a proxy of SFTPClient in order to support auto connection
   * and release feature. The ssh and sftp will be connected if the member method called first time,
   * and the sftp and ssh will be closed if the instance is not used for a while.
   *
   * @param uri the {@link AlluxioURI} for this UFS
   * @param ufsConf UFS configuration
   * @return the generated instance
   * @throws IOException
   */
  public static SFTPClient createSftpClient(AlluxioURI uri, UnderFileSystemConfiguration ufsConf)
      throws IOException {
    SftpContainer container = new SftpContainer(uri, ufsConf);
    sLiveContainers.add(container);
    return container.getSftpClientProxy();
  }

  /**
   * Create a remote file to an input stream, which will close the RemoteFile instance
   * in close method. And the remote file should not be accessed after transformation.
   *
   * @param remoteFile the remote file
   * @param offset the offset
   * @return the input stream
   * @throws IOException
   */
  public static InputStream createInputStream(RemoteFile remoteFile, long offset)
      throws IOException {
    Enhancer enhancer = new Enhancer();
    enhancer.setCallback(new CloseInterceptor(remoteFile));
    enhancer.setSuperclass(RemoteFile.ReadAheadRemoteFileInputStream.class);

    // The returned instance is a proxy of remoteFile.new ReadAheadRemoteFileInputStream(int, long),
    // and the proxy behavior is to close the remoteFile in close method
    return (InputStream) enhancer.create(
        new Class[] {RemoteFile.class, int.class, long.class},
        new Object[] {remoteFile, STREAM_CACHE_RW_NUM, offset});
  }

  /**
   * Create a remote file to an output stream, which will close the RemoteFile instance
   * in close method. And the remote file should not be accessed after transformation.
   *
   * @param remoteFile the remote file
   * @return the output stream
   * @throws IOException
   */
  public static OutputStream createOutputStream(RemoteFile remoteFile)
      throws IOException {
    Enhancer enhancer = new Enhancer();
    enhancer.setCallback(new CloseInterceptor(remoteFile));
    enhancer.setSuperclass(RemoteFile.RemoteFileOutputStream.class);

    // The returned instance is a proxy of remoteFile.new RemoteFileOutputStream(long, int),
    // and the proxy behavior is to close the remoteFile in close method
    return (OutputStream) enhancer.create(
        new Class[] {RemoteFile.class, long.class, int.class},
        new Object[] {remoteFile, 0, STREAM_CACHE_RW_NUM});
  }

  static {
    long delay =  MAX_IDLE_INTERVAL / 2;
    sExecutor.scheduleAtFixedRate(() -> scanIdle(), delay, delay, TimeUnit.MILLISECONDS);
  }

  protected static void scanIdle() {
    sLiveContainers.stream().forEach(container -> container.closeIfIdle());
  }

  /**
   * The container class of SFTP, which used to record the parameters used to
   * build SFTP connection, and maintain the connection instance.
   */
  private static class SftpContainer {
    private SSHClient mSshClient;
    private SFTPClient mSftpClient;
    private SFTPClient mSftpClientProxy;

    private String mUsername;
    private String mPassword;
    private String mHost;
    private int mPort;

    private long mLastActiveTime = 0;
    private Set<Object> mClientReferers = new HashSet<>();

    public SftpContainer(AlluxioURI uri, UnderFileSystemConfiguration ufsConf) {
      mUsername = SSH_GUEST_USER;
      if (ufsConf.containsKey(SshUFSPropertyKey.SSH_USERNAME)) {
        mUsername = ufsConf.getValue(SshUFSPropertyKey.SSH_USERNAME);
      }
      mPassword = "";
      if (ufsConf.containsKey(SshUFSPropertyKey.SSH_PASSWORD)) {
        mPassword = ufsConf.getValue(SshUFSPropertyKey.SSH_PASSWORD);
      }

      mHost = uri.getHost();
      mPort = -1;
      if (uri.getPort() > 0) {
        mPort = uri.getPort();
      }
    }

    /**
     * It is supposed that the sftp client can be connected when methods called and closed
     * after a while automatically. The best implementation is to proxy the client, connect
     * before each call of method, and close the connection after a certain time.
     *
     * @return the proxy client
     * @throws IOException
     */
    public SFTPClient getSftpClientProxy() throws IOException {
      SFTPClient client = connectSftp();

      Enhancer enhancer = new Enhancer();
      enhancer.setCallback(new SftpClientInterceptor());
      enhancer.setSuperclass(SFTPClient.class);
      mSftpClientProxy = (SFTPClient) enhancer.create(
          new Class[]{SFTPEngine.class}, new Object[]{client.getSFTPEngine()});

      return mSftpClientProxy;
    }

    private synchronized SFTPClient connectSftp() throws IOException {
      if (mLastActiveTime > 0) {
        mLastActiveTime = System.currentTimeMillis();
        return mSftpClient;
      }

      LOG.info("will connect sftp to host: " + mHost);
      mSshClient = new SSHClient();
      // do not verify the public key of remote
      mSshClient.addHostKeyVerifier(new PromiscuousVerifier());
      if (mPort > 0) {
        mSshClient.connect(mHost, mPort);
      } else {
        mSshClient.connect(mHost);
      }

      if (Strings.isNullOrEmpty(mPassword)) {
        mSshClient.authPublickey(mUsername);
      } else {
        mSshClient.authPassword(mUsername, mPassword);
      }
      mSftpClient = mSshClient.newSFTPClient();

      mClientReferers.clear();
      mLastActiveTime = System.currentTimeMillis();
      return mSftpClient;
    }

    private void closeIfIdle() {
      Closer closer;
      long curTime = System.currentTimeMillis();
      synchronized (this) {
        if (mLastActiveTime == 0 // closed
            || !mClientReferers.isEmpty() || curTime - mLastActiveTime < MAX_IDLE_INTERVAL) {
          return;
        }
        closer = Closer.create();
        closer.register(mSshClient);
        closer.register(mSftpClient);
        mSshClient = null;
        mSftpClient = null;
        mLastActiveTime = 0;
      }

      LOG.info("will close sftp connect to host: " + mHost);
      try {
        closer.close();
      } catch (IOException e) {
        LOG.error("close ssh connection failed of host: " + mHost, e);
      }
    }

    private synchronized void incReference(Object refer) {
      Preconditions.checkArgument(mSftpClient != null,
          mHost + " sftp not connected or connection closed, maybe idle interval is too small");
      mClientReferers.add(refer);
      mLastActiveTime = System.currentTimeMillis();
    }

    private synchronized boolean decReference(Object refer) {
      if (!mClientReferers.contains(refer)) {
        return false;
      }
      mClientReferers.remove(refer);
      mLastActiveTime = System.currentTimeMillis();
      return true;
    }

    private RemoteFile proxyRemoteFile(RemoteFile original, SFTPClient sftpClient) {
      Enhancer enhancer = new Enhancer();
      enhancer.setCallback(new RemoteFileInterceptor(original));
      enhancer.setSuperclass(RemoteFile.class);
      return (RemoteFile) enhancer.create(
          new Class[]{SFTPEngine.class, String.class, byte[].class},
          new Object[]{sftpClient.getSFTPEngine(), null, null});
    }

    /**
     * Sftp will be connected for the first time.
     * And the last access time is updated, a reference of current client
     * is recorded if getting a closeable instance, these two conditions
     * are used to determine whether the connection should be closed
     * after a while.
     */
    private class SftpClientInterceptor implements MethodInterceptor {
      @Override
      public Object intercept(
          Object object,
          Method method,
          Object[] objects,
          MethodProxy methodProxy) throws Throwable {

        SFTPClient sftpClient = connectSftp();

        Object ret = methodProxy.invoke(sftpClient, objects);
        if (ret == null || method.getName().startsWith("get")) {
          return ret;
        } else if (RemoteFile.class.isAssignableFrom(ret.getClass())) {
          incReference(ret);
          return proxyRemoteFile((net.schmizz.sshj.sftp.RemoteFile) ret, sftpClient);
        } else if (AutoCloseable.class.isAssignableFrom(ret.getClass())) {
          throw new RuntimeException("Not supported method by the proxy: " + method);
        }
        return ret;
      }
    }

    /**
     * The reference of sftp client is released in close method,
     * so the client can be detected to be idle and then closed.
     */
    private class RemoteFileInterceptor implements MethodInterceptor {
      private RemoteFile mOriginalRemoteFile;

      /**
       * Constructs an interceptor.
       *
       * @param originalRemoteFile the original instance
       */
      public RemoteFileInterceptor(RemoteFile originalRemoteFile) {
        mOriginalRemoteFile = originalRemoteFile;
      }

      @Override
      public Object intercept(
          Object object,
          Method method,
          Object[] objects,
          MethodProxy methodProxy) throws Throwable {
        if (!CLOSE_METHOD.equals(method.getName()) || method.getParameterCount() != 0) {
          return methodProxy.invoke(mOriginalRemoteFile, objects);
        }
        if (decReference(mOriginalRemoteFile)) {
          // the close method of RemoteFile is not reenterable
          mOriginalRemoteFile.close();
        }
        return null;
      }
    }
  }

  /**
   * This class is used to generated a integrated stream, to close
   * the corresponding RemoteFile instance in the close method.
   */
  private static class CloseInterceptor implements MethodInterceptor {
    private AutoCloseable mCloseable;

    /**
     * Constructs a new {@link CloseInterceptor}.
     *
     * @param closeable The closeable object used in close method
     */
    public CloseInterceptor(AutoCloseable closeable) {
      mCloseable = closeable;
    }

    @Override
    public Object intercept(Object object, Method method, Object[] objects, MethodProxy methodProxy)
        throws Throwable {
      if (!CLOSE_METHOD.equals(method.getName()) || method.getParameterCount() != 0) {
        return methodProxy.invokeSuper(object, objects);
      }

      try {
        return methodProxy.invokeSuper(object, objects);
      } finally {
        mCloseable.close();
      }
    }
  }
}
