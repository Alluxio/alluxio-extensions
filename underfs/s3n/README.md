## S3N Under Storage (Legacy)

Alluxio under storage implementation for accessing Amazon S3 using the `s3n://` scheme. This module is built using the jets3t library.

### Build

```bash
mvn package
```

### Run Integration Tests

```bash
mvn test -DtestS3Bucket=s3n://<bucket-name>/<folder> -Dfs.s3n.awsAccessKeyId=<access-key> -Dfs.s3n.awsSecretAccessKey=<secret-key>
```

### Mount

```bash
./bin/alluxio fs mount --option fs.s3n.awsAccessKeyId==<access-key> --option fs.s3n.awsSecretAccessKey==<secret-key>\
  /mnt/s3n s3n://<S3_BUCKET>/<S3_DIRECTORY>
```
