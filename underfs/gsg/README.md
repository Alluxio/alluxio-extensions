## Google cloud storage with Google cloud API

GSG implementation accessing Google cloud storage using the `gsg://` scheme.

### Build

Run the following command to build the GSG extension jar.

```bash
mvn package -DskipTests
```

### Run Under Storage Contract Tests

Note: The following runs a test to verify that the under storage satisfies the contract with Alluxio.
Without the option `testGSGBucket` only unit tests are triggered.

```bash
mvn test -DtestGSGBucket=gsg://<bucket>
```

### Deploy

```bash
/bin/alluxio extensions install <path>/<to>/alluxio-extensions/underfs/gsg/target/alluxio-underfs-gsg-<version>.jar
```

### Mount

```bash
./bin/alluxio fs mount /mnt/gsg gsg://<bucket>
```
