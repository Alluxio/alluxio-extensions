package alluxio.underfs.seaweedfs;

import alluxio.underfs.options.OpenOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import seaweedfs.client.FilerGrpcClient;
import seaweedfs.client.FilerProto;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author DPn!ce
 */
public class SeaweedfsInputStream extends InputStream {
    private static final Logger LOG = LoggerFactory.getLogger(SeaweedfsInputStream.class);

    private InputStream bais;

    public SeaweedfsInputStream(FilerGrpcClient filerGrpcClient, FilerProto.Entry entry, OpenOptions openOptions) {

        List<FilerProto.FileChunk> chunksList = entry.getChunksList();
        long offset = openOptions.getOffset();
        AtomicReference<Long> skipN = new AtomicReference<>(0L);
        List<FilerProto.FileChunk> filterFileChunks = Arrays.asList(chunksList.stream().filter(fileChunk -> {
            if (fileChunk.getOffset() <= offset && offset < fileChunk.getSize()) {
                //块的偏移量
                skipN.set(offset - fileChunk.getOffset());
                return true;
            } else {
                return offset < fileChunk.getSize();
            }
        }).toArray(FilerProto.FileChunk[]::new));
        //从指定位置打开
        long start = System.currentTimeMillis();
        if (filterFileChunks.size() == 0) {
            //fix empty files
            filterFileChunks = chunksList;
        }
        try {
            bais = SeaweedRead.read(filerGrpcClient, filterFileChunks);
            long skip = bais.skip(skipN.get());
            if (skip == bais.available()) {
                LOG.error("skip {} => ：InputStream number of bytes {}", offset, bais.available());
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        System.out.println("read time:" + (System.currentTimeMillis() - start) + " ms");

    }

    @Override
    public int read() throws IOException {
        LOG.info("=================read()=========================");
        return bais.read();
    }

    @Override
    public int read(byte[] b) throws IOException {
        LOG.info("=================read(byte[] b)=========================");
        return bais.read(b);
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        LOG.info("=================read(byte[] b, int off, int len)=========================");
        LOG.info("偏移量为：{},长度为: {}", off, len);
        return bais.read(b, off, len);
    }

    @Override
    public long skip(long n) throws IOException {
        LOG.info("=================skip(long n)=========================");
        return bais.skip(n);
    }

    @Override
    public int available() throws IOException {
        LOG.info("=================available()=========================");
        return bais.available();
    }

    @Override
    public void close() throws IOException {
        LOG.info("=================close()=========================");
        bais.close();
    }


}
