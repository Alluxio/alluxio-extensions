package alluxio.underfs.seaweedfs;

import alluxio.AlluxioURI;
import alluxio.underfs.UnderFileSystem;
import alluxio.underfs.UnderFileSystemConfiguration;
import alluxio.underfs.UnderFileSystemFactory;
import com.google.common.base.Preconditions;

import javax.annotation.Nullable;
/**
 * SeaweedfsUnderFileSystemFactory
 *
 * @author DPnice
 * @date 2018/12/24
 */
public class SeaweedfsUnderFileSystemFactory implements UnderFileSystemFactory {
    @Override
    public UnderFileSystem create(String path, @Nullable UnderFileSystemConfiguration conf) {
        // Create the under storage instance
        Preconditions.checkNotNull(path, "path");
        // conf 可以自定义配置
        return new SeaweedfsUnderFileSystem(new AlluxioURI(path), conf);
    }

    @Override
    public boolean supportsPath(String path) {
        return path.startsWith("seaweedfs://");
    }

}
