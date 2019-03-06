## Seaweedfs Under Storage

Alluxio支持seaweedfs作为底层存储的扩展

### Build

```bash
mvn package
```

### Run Integration Tests

```bash
mvn test -Dseaweedfs.client.pool.size=10  -Dseaweedfs.replication=000
```
### Install

```bash
bin/alluxio extensions install alluxio-underfs-seaweedfs-1.8.1.jar
```

### List

```bash
bin/alluxio extensions ls
```

### Mount

```bash
bin/alluxio fs mount /seaweedfs-storage seaweedfs://<host-name>:8888/ --option seaweedfs.replication=000 --option seaweedfs.client.pool.size=10
```
> The host-name is the name of the filer server,8888 is the default port.

### RunTests

```bash
bin/alluxio runTests --directory /seaweedfs-storage
```

### Unmount

```bash
bin/alluxio fs unmount /seaweedfs-storage
```

### Uninstall

```bash
bin/alluxio extensions uninstall alluxio-underfs-seaweedfs-1.8.1.jar
```