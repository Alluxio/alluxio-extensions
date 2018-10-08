## SSH Under Storage

Alluxio under storage implementation for accessing local file system using the `ssh://` scheme. This module is built using the sshj library, which is implemented by SFTP, a subprotocol of SSH.

### Build

```bash
mvn package
```

### Run Integration Tests

```bash
mvn test -DtestSshUFS=ssh://<host-name>/<folder> -Dssh.username=<username> -Dssh.password=<password>
```

### Mount

```bash
./bin/alluxio fs mount --option ssh.username=<username> --option ssh.password=<password> /mnt/ssh ssh://<host-name>/<folder>
```
