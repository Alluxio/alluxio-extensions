## Tutorial for Under Storage Extension Development

Dummy Alluxio under storage implementation accessing local file system using the `dummy://` scheme.

### Build

```bash
mvn package
```

### Run Integration Tests

```bash
mvn test -DtestTutorialUFS=dummy://<folder>
```

### Mount

```bash
./bin/alluxio fs mount /mnt/dummy dummy://<folder>
```
