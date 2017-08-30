## Tutorial for Under Storage Extension Development

Dummy Alluxio under storage implementation accessing local file system using the `dummy://` scheme.

### Build

```bash
mvn package
```

### Run Under Storage Contract Tests

Note: The following runs a test to verify that the under storage satisfies the contract with Alluxio.
Without the option `testTutorialUFS` only unit tests are triggered.

```bash
mvn test -DtestTutorialUFS=dummy://<folder>
```

### Deploy

```bash
/bin/alluxio extensions install <path>/<to>/alluxio-extensions/underfs/tutorial/target/alluxio-underfs-dummy-1.6.0-SNAPSHOT.jar
```

### Mount

```bash
./bin/alluxio fs mount /mnt/dummy dummy://<folder>
```
