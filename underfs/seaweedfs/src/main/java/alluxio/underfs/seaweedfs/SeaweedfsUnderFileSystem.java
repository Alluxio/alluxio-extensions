package alluxio.underfs.seaweedfs;

import alluxio.AlluxioURI;
import alluxio.security.authorization.Mode;
import alluxio.underfs.*;
import alluxio.underfs.options.*;
import alluxio.util.UnderFileSystemUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import seaweedfs.client.FilerClient;
import seaweedfs.client.FilerGrpcClient;
import seaweedfs.client.FilerProto;

import javax.annotation.concurrent.ThreadSafe;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.*;


/**
 * @author DPnice
 */
@ThreadSafe
public class SeaweedfsUnderFileSystem extends BaseUnderFileSystem {
    private static final Logger LOG = LoggerFactory.getLogger(SeaweedfsUnderFileSystemFactory.class);
    public UnderFileSystemConfiguration mUfsConf;
    private Vector<FilerGrpcClient> filerGrpcClientPool;
    private int filerGrpcClientPoolSize = 10;
    public static String replication = "000";
    private Random random = new Random();

    /**
     * Factory method to constructs a new seaweedfs {@link UnderFileSystem} instance.
     *
     * @param uri     the {@link AlluxioURI} for this UFS
     * @param ufsConf
     */
    protected SeaweedfsUnderFileSystem(AlluxioURI uri, UnderFileSystemConfiguration ufsConf) {
        super(uri, ufsConf);
        // TODO 添加多个filer的配置 或者配置nginx
        mUfsConf = ufsConf;
        LOG.info("AlluxioURI: {}", uri.toString());
        final String host = uri.getHost();
        final int port = 10000 + uri.getPort();
        //加入配置文件
        if (ufsConf.containsKey(SeaweedfsPropertyKey.SEAWEEDFS_REPLICATION)) {
            replication = ufsConf.getValue(SeaweedfsPropertyKey.SEAWEEDFS_REPLICATION);
        }
        if (ufsConf.containsKey(SeaweedfsPropertyKey.SEAWEEDFS_CLIENT_POOL_SIZE)) {
            filerGrpcClientPoolSize =
                    Integer.valueOf(ufsConf.getValue(SeaweedfsPropertyKey.SEAWEEDFS_CLIENT_POOL_SIZE)
                            .trim());
        }

        filerGrpcClientPool = new Vector<>(filerGrpcClientPoolSize);
        for (int i = 0; i < filerGrpcClientPoolSize; i++) {
            filerGrpcClientPool.add(new FilerGrpcClient(host, port));
        }
    }

    @Override
    public void connectFromMaster(String s) throws IOException {
        // No-op
    }

    @Override
    public void connectFromWorker(String s) throws IOException {
        // No-op
    }

    @Override
    public OutputStream create(String pathStr, CreateOptions createOptions) throws IOException {
        LOG.info("===========================create=============================");
        long now = System.currentTimeMillis() / 1000L;
        LOG.info("上传文件的mod: {}", createOptions.getMode().toString());
        AlluxioURI path = getPath(pathStr);
        LOG.info("create file name: {}", path.getName());
        FilerProto.Entry.Builder entry = FilerProto.Entry.newBuilder()
                .setName(path.getName())
                .setIsDirectory(false)
                .setAttributes(FilerProto.FuseAttributes.newBuilder()
                        .setFileMode(getMode(createOptions.getMode()))
                        .setReplication(replication)
                        .setCrtime(now)
                        .setMtime(now)
                        .setUserName(null != createOptions.getOwner() ? createOptions.getOwner() : System.getProperty("user.name"))
                        .clearGroupName()
                        .addAllGroupName(Collections.singletonList(createOptions.getGroup() != null ? createOptions.getGroup() : ""))
                );

        return new SeaweedfsOutputStream(getFilerGrpcClient(), path, entry);
    }

    /**
     * 删除目录
     *
     * @param path          路径
     * @param deleteOptions 是否递归
     * @return boolean
     */
    @Override
    public boolean deleteDirectory(String path, DeleteOptions deleteOptions) throws IOException {
        LOG.info("===========================deleteDirectory=============================");
        path = getPath(path).getPath();
        return isDirectory(path) && getFc().rm(path, deleteOptions.isRecursive());
    }

    /**
     * 删除文件
     */
    @Override
    public boolean deleteFile(String path) throws IOException {
        LOG.info("===========================deleteFile=============================");
        path = getPath(path).getPath();
        return isFile(path) && getFc().rm(path, false);
    }

    @Override
    public long getBlockSizeByte(String s) throws IOException {
        LOG.info("===========================getBlockSizeByte=============================");
        return 32 * 1024 * 1024;
    }

    @Override
    public UfsDirectoryStatus getDirectoryStatus(String path) throws IOException {
        LOG.info("===========================getDirectoryStatus=============================");
        FilerProto.Entry entry = getEntry(path);
        return new UfsDirectoryStatus(path, entry.getAttributes().getUserName(), entry.getAttributes().getGroupNameList().size() > 0 ? entry.getAttributes().getGroupName(0) : entry.getAttributes().getUserName(),
                (short) entry.getAttributes().getFileMode(), entry.getAttributes().getMtime() * 1000);
    }

    @Override
    public List<String> getFileLocations(String path) throws IOException {
        LOG.info("===========================getFileLocations=============================");
        return getFileLocations(path, FileLocationOptions.defaults());
    }

    @Override
    public List<String> getFileLocations(String path, FileLocationOptions fileLocationOptions) throws IOException {
        // TODO 可以解决 待解决 主要用于数据的本地性

        return null;
    }

    @Override
    public UfsFileStatus getFileStatus(String path) throws IOException {
        LOG.info("===========================getFileStatus=============================");
        if (!isFile(path)) {
            LOG.warn("path not not a file: {}", path);
            return null;
        }
        FilerProto.Entry entry = getEntry(path);
        String contentHash =
                UnderFileSystemUtils.approximateContentHash(entry.getAttributes().getFileSize(), entry.getAttributes().getMtime());

        return new UfsFileStatus(entry.getName(),
                contentHash,
                entry.getChunksList().stream().parallel().reduce((long) 0, (x, y) -> {
                    x += y.getSize();
                    return x;
                }, (x, y) -> x + y),
                entry.getAttributes().getMtime() * 1000,
                entry.getAttributes().getUserName(),
                entry.getAttributes().getGroupNameCount() > 0 ? entry.getAttributes().getGroupName(0) : "",
                (short) entry.getAttributes().getFileMode()
        );
    }

    @Override
    public long getSpace(String path, SpaceType spaceType) throws IOException {
        // TODO 这里可能曲线救国 使用 Volume API
        return 0;
    }

    @Override
    public UfsStatus getStatus(String path) throws IOException {
        LOG.info("===========================getStatus=============================");
        if (isFile(path)) {
            return getFileStatus(path);
        } else {
            return getDirectoryStatus(path);
        }
    }

    @Override
    public String getUnderFSType() {
        return "seaweedfs";
    }

    @Override
    public boolean isDirectory(String path) throws IOException {
        LOG.info("===========================isDirectory=============================");
        FilerProto.Entry entry = getEntry(path);
        return null != entry && entry.getIsDirectory();
    }

    @Override
    public boolean isFile(String path) throws IOException {
        LOG.info("===========================isFile=============================");
        FilerProto.Entry entry = getEntry(path);
        return null != entry && !entry.getIsDirectory();
    }

    /**
     * 文件列表
     *
     * @param path 路径
     * @return UfsStatus[]
     */
    @Override
    public UfsStatus[] listStatus(String path) throws IOException {
        LOG.info("==========================listStatus=============================");
        path = getPath(path).getPath();
        LOG.info("查看目录: {}", path);

        List<FilerProto.Entry> entries = getFc().listEntries(path);
        boolean flag = (!isDirectory(path) && !isFile(path)) || (entries.isEmpty() && isFile(path));
        if (flag) {
            return null;
        }
        return entries.stream().map(entry -> {
            UfsStatus retStatus;
            if (entry.getIsDirectory()) {
                retStatus = new UfsDirectoryStatus(entry.getName(),
                        entry.getAttributes().getUserName(),
                        entry.getAttributes().getGroupNameCount() > 0 ? entry.getAttributes().getGroupName(0) : "",
                        (short) entry.getAttributes().getFileMode(),
                        entry.getAttributes().getMtime() * 1000);
            } else {
                String contentHash = UnderFileSystemUtils
                        .approximateContentHash(entry.getAttributes().getFileSize(), entry.getAttributes().getMtime());
                retStatus = new UfsFileStatus(entry.getName(), contentHash,
                        entry.getChunksList().stream().parallel().reduce((long) 0, (x, y) -> {
                            x += y.getSize();
                            return x;
                        }, (x, y) -> x + y),
                        entry.getAttributes().getMtime() * 1000,
                        entry.getAttributes().getUserName(),
                        entry.getAttributes().getGroupNameCount() > 0 ? entry.getAttributes().getGroupName(0) : "",
                        (short) entry.getAttributes().getFileMode());
            }
            LOG.info("文件名称: {} 是否是目录: {} Mode: {}", retStatus.getName(), retStatus.isDirectory(), retStatus.getMode());
            return retStatus;
        }).toArray(UfsStatus[]::new);
    }

    @Override
    public boolean mkdirs(String path, MkdirsOptions mkdirsOptions) throws IOException {
        LOG.info("===========================mkdirs=============================");
        path = getPath(path).getPath();
        LOG.info("mkdirs: {}", path);
        Mode mode1 = mkdirsOptions.getMode();
        LOG.info("Owner: {}, Group: {}", mkdirsOptions.getOwner(), mkdirsOptions.getGroup(), mode1.getOwnerBits());
        LOG.info("mode: {}, getOwnerBits: {}", mode1.toString(), mode1.getOwnerBits());
        int mode = getMode(mkdirsOptions.getMode());
        LOG.info("mode转换成int: {}", mode);
        boolean flag = getFc().mkdirs(path, mode, null != mkdirsOptions.getOwner() ? mkdirsOptions.getOwner() : System.getProperty("user.name"),
                mkdirsOptions.getGroup() != null ? new String[]{mkdirsOptions.getGroup()} : new String[0]);
        LOG.info("是否创建成功: {}", flag);
        return flag;
    }

    @Override
    public InputStream open(String path, OpenOptions openOptions) throws IOException {
        FilerProto.Entry entry = getEntry(path);
        return new SeaweedfsInputStream(getFilerGrpcClient(), entry, openOptions);
    }

    @Override
    public boolean renameDirectory(String source, String destination) throws IOException {
        LOG.info("===========================renameDirectory=============================");
        LOG.info("renameDirectory source: {} destination:{}", source, destination);
        AlluxioURI path = getPath(source);
        if ("/".equals(path.getPath())) {
            LOG.warn("Can't be the root path source: {}", source);
            return false;
        }

        FilerProto.Entry entry = getEntry(source);
        if (entry == null) {
            LOG.warn("rename non-existing source: {}", source);
            return false;
        }
        LOG.info("rename moveEntry source: {}", source);

        return moveEntry(path.getParent(), entry, getPath(destination));
    }

    private boolean moveEntry(AlluxioURI oldParent, FilerProto.Entry entry, AlluxioURI destination) throws IOException {
        LOG.info("moveEntry: {}/{}  => {}", oldParent, entry.getName(), destination);
        FilerProto.Entry newEntry = entry.toBuilder().setName(destination.getName()).build();
        boolean isDirectoryCreated = getFc().
                createEntry(getPath(Objects.requireNonNull(destination.getParent()).toString()).getPath(), newEntry);

        if (!isDirectoryCreated) {
            return false;
        }

        if (entry.getIsDirectory()) {
            //当前目录
            String pathStr = oldParent.getPath() + "/" + entry.getName();
            //目录下都需要改名
            AlluxioURI entryPath = new AlluxioURI(pathStr);
            List<FilerProto.Entry> entries = getFc().listEntries(pathStr);
            for (FilerProto.Entry ent : entries) {
                boolean success = moveEntry(entryPath, ent, getPath(destination.toString() + "/" + ent.getName()));
                if (!success) {
                    return false;
                }
            }
        }
        return getFc().deleteEntry(
                oldParent.getPath(), entry.getName(), false, false);
    }

    @Override
    public boolean renameFile(String oldPath, String newPath) throws IOException {
        LOG.info("===============renameFile=================");
        LOG.info("String oldPath :{}, String newPath :{}", oldPath, newPath);
        // 只更新文件名的引用 不更改实际存储
        FilerProto.Entry entry = getEntry(oldPath);
        if (entry == null) {
            LOG.warn("file :{} 不存在", oldPath);
            return false;
        }
        AlluxioURI path = getPath(newPath);
        FilerProto.Entry newEntry = entry.toBuilder().setName(path.getName()).build();
        AlluxioURI newPathParent = getPath(path.getPath()).getParent();
        boolean createBoolean = getFc().createEntry(Objects.requireNonNull(newPathParent).toString(),
                newEntry);
        if (!createBoolean) {
            LOG.warn("file :{} 更新失败", oldPath);
            return false;
        }
        AlluxioURI path1 = getPath(oldPath);
        return getFc().deleteEntry(Objects.requireNonNull(getPath(path1.getPath()).getParent()).toString(),
                path1.getName(), false, true);
    }

    @Override
    public void setMode(String path, short mode) throws IOException {
        LOG.info("===========================setMode=============================");
        LOG.info("setMode path:{} mode:{}", path, mode);
        FilerProto.Entry entry = getEntry(path);
        if (entry == null) {
            LOG.debug("setMode path:{} entry:{}", path, entry);
            return;
        }
        FilerProto.Entry.Builder entryBuilder = entry.toBuilder();
        FilerProto.FuseAttributes.Builder attributesBuilder = entry.getAttributes().toBuilder();
        attributesBuilder.setFileMode(mode);
        entryBuilder.setAttributes(attributesBuilder);
        LOG.info("setMode path:{} entry:{}", path, entryBuilder);
        getFc().updateEntry(Objects.requireNonNull(getPath(path).getParent()).toString(),
                entryBuilder.build());
    }

    @Override
    public void setOwner(String path, String user, String group) throws IOException {
        LOG.info("===========================setOwner=============================");
        LOG.info("setOwner path:{} owner:{} group:{}", path, user, group);
        FilerProto.Entry entry = getEntry(path);
        if (entry == null) {
            LOG.warn("setOwner path:{} entry:{}", path, entry);
            return;
        }
        FilerProto.Entry.Builder entryBuilder = entry.toBuilder();
        FilerProto.FuseAttributes.Builder attributesBuilder = entry.getAttributes().toBuilder();
        if (user != null) {
            attributesBuilder.setUserName(user);
        }
        if (group != null) {
            attributesBuilder.clearGroupName();
            attributesBuilder.addGroupName(group);
        }
        entryBuilder.setAttributes(attributesBuilder);
        LOG.info("setOwner path:{} entry:{}", path, entryBuilder);
        getFc().updateEntry(Objects.requireNonNull(getPath(path).getParent()).toString(),
                entryBuilder.build());
    }

    @Override
    public boolean supportsFlush() {
        return true;
    }

    @Override
    public void close() throws IOException {
        filerGrpcClientPool.forEach(f -> {
            try {
                f.shutdown();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        });

    }

    public FilerClient getFc() {
        return new FilerClient(getFilerGrpcClient());
    }

    public FilerGrpcClient getFilerGrpcClient() {
        return filerGrpcClientPool.get(random.nextInt(filerGrpcClientPoolSize));
    }


    /**
     * 获得Entry的详情
     *
     * @param path Entry的路径
     * @return Entry
     */
    private FilerProto.Entry getEntry(String path) throws IOException {
        AlluxioURI pathObject = getPath(path);
        String pathStr = pathObject.getPath();
        LOG.info("path is: {}", pathStr);
        String fileName = "/".equals(pathStr) ? "/" : pathObject.getName();
        String parent = "/".equals(pathStr) ? "/" : Objects.requireNonNull(new AlluxioURI(pathStr).getParent()).getPath();

        return getFc().lookupEntry(parent, fileName);
    }

    private AlluxioURI getPath(String path) {
        //去掉前缀
        return new AlluxioURI(path);
    }

    /**
     * 将 mode 转成 int
     *
     * @param mode mode 权限
     * @return int 对应的权限数
     */
    private int getMode(Mode mode) {
        return mode.getOwnerBits().ordinal() << 6 | mode.getGroupBits().ordinal() << 3 | mode.getOtherBits().ordinal();
    }


}
