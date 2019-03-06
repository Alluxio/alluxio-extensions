package alluxio.underfs.seaweedfs;

import alluxio.AlluxioURI;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.mime.HttpMultipartMode;
import org.apache.http.entity.mime.MultipartEntityBuilder;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import seaweedfs.client.FilerGrpcClient;
import seaweedfs.client.FilerProto;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Objects;

import static java.nio.charset.StandardCharsets.UTF_8;
/**
 * SeaweedWrite
 *
 * @author DPn!ce
 * @date 2018/12/24
 */
public class SeaweedWrite {
    private static final Logger LOG = LoggerFactory.getLogger(SeaweedWrite.class);

    public static void writeData(FilerProto.Entry.Builder entry,
                                 final FilerGrpcClient filerGrpcClient,
                                 final long offset,
                                 final byte[] bytes,
                                 final long bytesLength) throws IOException {
        FilerProto.AssignVolumeResponse response = filerGrpcClient.getBlockingStub().assignVolume(
                FilerProto.AssignVolumeRequest.newBuilder()
//                        .setCollection("")
//                        .setDataCenter("")
                        .setReplication(SeaweedfsUnderFileSystem.replication)
//                        .setTtlSec(0)
                        .build());
        String fileId = response.getFileId();
        String url = response.getUrl();
        String targetUrl = String.format("http://%s/%s", url, fileId);
        LOG.info("文件上传URL: {}", targetUrl);
        String eTag = multipartUpload(targetUrl, bytes, 0, bytesLength);
        LOG.info("上传成功eTag: {}", eTag);
        entry.addChunks(FilerProto.FileChunk.newBuilder()
                .setFileId(fileId)
                .setOffset(offset)
                .setSize(bytesLength)
                .setMtime(System.currentTimeMillis() / 10000L)
                .setETag(eTag)
        );

    }

    /**
     * 将文件的元数据信息保存在filer(数据库)
     *
     * @param filerGrpcClient Grpc Client
     * @param path            路径
     * @param entry           文件描述类
     */
    public static void writeMeta(final FilerGrpcClient filerGrpcClient,
                                 final AlluxioURI path, final FilerProto.Entry.Builder entry) {
        String pathStr = Objects.requireNonNull(new AlluxioURI(path.getPath()).getParent()).toString();
        LOG.info("创建元数据名称: {}", entry.getName());
        LOG.info("创建元数据路径: {}", pathStr);
        LOG.info("元数据: {}", entry);
        filerGrpcClient.getBlockingStub().createEntry(
                FilerProto.CreateEntryRequest.newBuilder()
                        .setDirectory(pathStr)
                        .setEntry(entry)
                        .build()
        );
    }

    /**
     * 上传
     *
     * @param targetUrl 目标url
     * @param bytes bytes[]
     * @param bytesOffset 上传数组的偏移量
     * @param bytesLength 上传数组的长度
     * @return
     * @throws IOException
     */
    private static String multipartUpload(String targetUrl,
                                          final byte[] bytes,
                                          final long bytesOffset,
                                          final long bytesLength) throws IOException {
        LOG.info("==========================multipartUpload==========================");
        LOG.info(" bytesOffset: {}, bytesLength: {}", bytesOffset, bytesLength);
        // 自动client资源管理
        try (CloseableHttpClient client = HttpClientBuilder.create().setUserAgent("alluxio-client").build()) {
            InputStream inputStream = new ByteArrayInputStream(bytes, (int) bytesOffset, (int) bytesLength);

            HttpPost post = new HttpPost(targetUrl);

            post.setEntity(MultipartEntityBuilder.create()
                    .setMode(HttpMultipartMode.BROWSER_COMPATIBLE)
                    .setCharset(UTF_8)
                    .addBinaryBody("upload", inputStream)
                    .build());

            HttpResponse response = client.execute(post);
            String eTag = response.getLastHeader("ETag").getValue();
            if (eTag != null && eTag.startsWith("\"") && eTag.endsWith("\"")) {
                return eTag.substring(1, eTag.length() - 1);
            }

            return eTag;
        }

    }
}
