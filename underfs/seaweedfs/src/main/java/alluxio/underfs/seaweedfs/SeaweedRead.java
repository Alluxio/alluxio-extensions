package alluxio.underfs.seaweedfs;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.HttpClientBuilder;
import seaweedfs.client.FilerGrpcClient;
import seaweedfs.client.FilerProto;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.SequenceInputStream;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

/**
 * SeaweedRead
 *
 * @author DPn!ce
 * @date 2018/12/24
 */
public class SeaweedRead {

    public static InputStream read(FilerGrpcClient filerGrpcClient,
                                   List<FilerProto.FileChunk> chunksList) {
        //通过chunksList构造lookupRequest
        FilerProto.LookupVolumeRequest.Builder reduceLookupRequest = chunksList
                .stream().reduce(FilerProto.LookupVolumeRequest.newBuilder(), (x, y) -> {
                    //添加要读取的 fid 的volume 例如 [fid：2,1c391a59c030 ], volume为 2
                    x.addVolumeIds(parseVolumeId(y.getFileId()));
                    return x;
                }, (x, y) -> x.addAllVolumeIds(y.getVolumeIdsList()));

        // 返回对应fid 的 位置(主机地址)
        FilerProto.LookupVolumeResponse lookupResponse = filerGrpcClient
                .getBlockingStub().lookupVolume(reduceLookupRequest.build());

        // volumeId,locations
        Map<String, FilerProto.Locations> vid2Locations = lookupResponse.getLocationsMapMap();

        TreeMap<Integer, InputStream> collect1 = chunksList.parallelStream().collect(TreeMap::new, (map, fileChunk) -> {
            FilerProto.Locations locations = vid2Locations.get(parseVolumeId(fileChunk.getFileId()));
            HttpClient client = HttpClientBuilder.create().build();
            HttpGet request = new HttpGet(
                    String.format("http://%s/%s",
                            //locations {url: "cdh3:9222" , public_url: "cdh3"}
                            locations.getLocations(0).getUrl(),
                            fileChunk.getFileId())
            );
            HttpResponse response = null;
            try {
                response = client.execute(request);
            } catch (IOException e) {
                e.printStackTrace();
            }
            HttpEntity entity = Objects.requireNonNull(response).getEntity();

            try {
                InputStream content = entity.getContent();
                map.put((int) fileChunk.getOffset(), content);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }, TreeMap::putAll);

        final InputStream[] sequenceInputStream = {null};
        collect1.forEach((offset, inputStream) -> {
            if (sequenceInputStream[0] == null) {
                sequenceInputStream[0] = new SequenceInputStream(inputStream, new ByteArrayInputStream(new byte[0]));
            } else {
                try {
                    sequenceInputStream[0] = sequenceInputStream(sequenceInputStream[0], inputStream);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        });
        return sequenceInputStream[0];
    }

    public static String parseVolumeId(String fileId) {
        int commaIndex = fileId.lastIndexOf(',');
        if (commaIndex > 0) {
            return fileId.substring(0, commaIndex);
        }
        return fileId;
    }


    public static InputStream sequenceInputStream(InputStream sequenceInputStream, InputStream content) throws IOException {
        return new SequenceInputStream(sequenceInputStream, content);
    }

}
