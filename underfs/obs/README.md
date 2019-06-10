## Huawei OBS Under Storage

Huawei OBS under storage implementation accessing Huawei OBS using the `obs://` scheme.

### Build

Huawei OBS SDK can only be downloaded from Huawei Cloud's own maven repository, you need to register [here](https://mirrors.huaweicloud.com/) 
and follow the instructions to update your maven settings.

After configuring your maven to be able to access Huawei Cloud's maven repository, run the following command.

```bash
mvn package -DskipTests
```

### Deploy

```bash
./bin/alluxio extensions install <path>/<to>/alluxio-extensions/underfs/obs/target/alluxio-underfs-obs-<version>.jar
```

### Run Under Storage Contract Tests

The following runs tests to verify that the under storage satisfies the contract with Alluxio.

```bash
./bin/alluxio runUfsTests --path obs://<bucket> -Dfs.obs.accessKey=<access_key> -Dfs.obs.secretKey=<secret_key> -Dfs.obs.endpoint=<endpoint>
```

### Mount

```bash
./bin/alluxio fs mount /mnt/obs obs://<bucket>
```
