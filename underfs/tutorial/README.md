## Tutorial for Under Storage Extension Development

Dummy Alluxio under storage implementation accessing local file system using the `dummy://` scheme.

### Build

```bash
mvn package -DskipTests
```

### Deploy

```bash
/bin/alluxio extensions install <path>/<to>/alluxio-extensions/underfs/tutorial/target/alluxio-underfs-dummy-1.6.0-SNAPSHOT.jar
```

### Run Under Storage Contract Tests

The following runs tests to verify that the under storage satisfies the contract with Alluxio.

```bash
./bin/alluxio runUfsTests --path dummy://<folder>
```

### Mount

```bash
./bin/alluxio fs mount /mnt/dummy dummy://<folder> --option fs.dummy.sleep=1s
```
