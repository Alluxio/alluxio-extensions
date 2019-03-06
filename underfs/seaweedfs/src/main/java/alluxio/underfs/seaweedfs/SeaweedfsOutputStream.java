package alluxio.underfs.seaweedfs;

import alluxio.AlluxioURI;
import org.apache.http.util.ByteArrayBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import seaweedfs.client.FilerGrpcClient;
import seaweedfs.client.FilerProto;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author DPn!ce
 */
public class SeaweedfsOutputStream extends OutputStream {
    private static final Logger LOG = LoggerFactory.getLogger(SeaweedfsOutputStream.class);
    private final FilerGrpcClient filerGrpcClient;
    private final AlluxioURI path;
    private Integer chunkSize;
    private ByteArrayBuffer arrayBuffer;


    private FilerProto.Entry.Builder entry;
    private AtomicBoolean mClosed = new AtomicBoolean(false);

    public SeaweedfsOutputStream(FilerGrpcClient filerGrpcClient, AlluxioURI path, FilerProto.Entry.Builder entry) {
        super();
        this.filerGrpcClient = filerGrpcClient;
        this.path = path;
        this.entry = entry;
        chunkSize = 32 * 1024 * 1024;
        arrayBuffer = new ByteArrayBuffer(chunkSize);
    }

    @Override
    public void write(int b) throws IOException {
        LOG.info("=========================write(int b)============================");
        write(new byte[]{(byte) (b & 0xFF)});
    }

    @Override
    public void write(byte[] b) throws IOException {
        LOG.info("=========================write(byte[] b)============================");
        write(b, 0, b.length);
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        LOG.info("write off:{},len:{}", off, len);
        arrayBuffer.append(b, off, len);
    }


    @Override
    public void close() {
        byte[] allBytes = arrayBuffer.toByteArray();
        //关闭流的时候将文件块上传
        if (mClosed.getAndSet(true)) {
            LOG.info("======mClosed====={}", mClosed.get());
            return;
        }
        List<CustomFileChunk> bytesArrayList = new ArrayList<>();

        long cutStart = System.currentTimeMillis();
        int length = allBytes.length;
        int offset = 0;
        do {
            if (length <= chunkSize) {
                byte[] bytes = new byte[length];
                System.arraycopy(allBytes, offset, bytes, 0, length);
                bytesArrayList.add(new CustomFileChunk(offset, bytes));
                break;
            } else {
                byte[] bytes = new byte[chunkSize];
                System.arraycopy(allBytes, offset, bytes, 0, chunkSize);
                bytesArrayList.add(new CustomFileChunk(offset, bytes));
            }
            offset += chunkSize;
            length -= chunkSize;
        } while (true);
        long cutEnd = System.currentTimeMillis();
        System.out.println("cut time: " + (cutEnd - cutStart) + " ms");
        allBytes = null;

        // 并行上传
        bytesArrayList.parallelStream().forEach(cfc -> {
            try {
                SeaweedWrite.writeData(entry, filerGrpcClient, cfc.getOffset(),
                        cfc.getByteChunk(), cfc.getByteChunk().length);
            } catch (IOException e) {
                LOG.error("偏移量为 :{} ,上传块失败", cfc);
                e.printStackTrace();
            }
        });
        // 写入元数据
        SeaweedWrite.writeMeta(filerGrpcClient, path, entry);

        System.out.println("up date time: " + (System.currentTimeMillis() - cutEnd) + " ms");
    }

    /**
     * 合并byte[]
     *
     * @param first  byte[]
     * @param second byte[]
     * @return 合并之后新的byte[]
     */
    private static byte[] byteMerger(byte[] first, byte[] second) {
        byte[] result = Arrays.copyOf(first, first.length + second.length);
        System.arraycopy(second, 0, result, first.length, second.length);
        return result;
    }

}

class CustomFileChunk {
    private Integer offset;
    private byte[] byteChunk;

    public CustomFileChunk(Integer offset, byte[] byteChunk) {
        this.offset = offset;
        this.byteChunk = byteChunk;
    }

    public Integer getOffset() {
        return offset;
    }

    public void setOffset(Integer offset) {
        this.offset = offset;
    }

    public byte[] getByteChunk() {
        return byteChunk;
    }

    public void setByteChunk(byte[] byteChunk) {
        this.byteChunk = byteChunk;
    }
}

