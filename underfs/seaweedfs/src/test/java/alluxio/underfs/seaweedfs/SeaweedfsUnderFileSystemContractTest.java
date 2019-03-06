package alluxio.underfs.seaweedfs;

import alluxio.AlluxioURI;
import alluxio.underfs.AbstractUnderFileSystemContractTest;
import alluxio.underfs.UnderFileSystem;
import alluxio.underfs.UnderFileSystemConfiguration;
import alluxio.underfs.options.DeleteOptions;
import alluxio.util.io.PathUtils;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.UUID;

/**
 * @author DPn!ce
 * @date 2019/03/04.
 */
public class SeaweedfsUnderFileSystemContractTest extends AbstractUnderFileSystemContractTest {
    @Override
    public UnderFileSystem createUfs(String s, UnderFileSystemConfiguration conf) {
        HashMap<String, String> confMap = new HashMap<>();
//        Configuration.set(PropertyKey.USER_BLOCK_SIZE_BYTES_DEFAULT, "32");
        conf.setUserSpecifiedConf(confMap);
        return new SeaweedfsUnderFileSystem(new AlluxioURI(s), conf);

    }

    @Override
    public String getUfsBaseDir() {
        return "seaweedfs://cdh1:8888";
    }

    //    @Test
    public void clear() {
        String mUnderfsAddress = PathUtils.concatPath(this.getUfsBaseDir(), new Object[]{UUID.randomUUID()});
        UnderFileSystem mUfs = createUfs(mUnderfsAddress, UnderFileSystemConfiguration.defaults());
        try {
            Arrays.asList(mUfs.listStatus("/")).forEach(f -> {
                try {
                    if (f.isDirectory()) {
                        mUfs.deleteDirectory("/" + f.getName(), DeleteOptions.defaults().setRecursive(true));
                    } else {
                        mUfs.deleteFile("/" + f.getName());
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
            });
        } catch (IOException e) {
            e.printStackTrace();
        }finally {
            try {
                mUfs.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

    }

}
