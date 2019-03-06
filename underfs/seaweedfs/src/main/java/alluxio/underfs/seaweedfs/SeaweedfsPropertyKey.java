package alluxio.underfs.seaweedfs;

import alluxio.PropertyKey;

import javax.annotation.concurrent.ThreadSafe;

/**
 * @author DPn!ce
 * @date 2019/03/06.
 */
@ThreadSafe
public class SeaweedfsPropertyKey {

    public static final PropertyKey SEAWEEDFS_REPLICATION =
            new PropertyKey.Builder(ConfName.SEAWEEDFS_REPLICATION)
                    .setDescription("seaweedfs reproduction rules default '000'")
                    .build();

    public static final PropertyKey SEAWEEDFS_CLIENT_POOL_SIZE =
            new PropertyKey.Builder(ConfName.SEAWEEDFS_CLIENT_POOL_SIZE)
                    .setDescription("default 10")
                    .build();

    @ThreadSafe
    public static final class ConfName {
        public static final String SEAWEEDFS_REPLICATION = "seaweedfs.replication";
        public static final String SEAWEEDFS_CLIENT_POOL_SIZE = "seaweedfs.client.pool.size";

    }
}
