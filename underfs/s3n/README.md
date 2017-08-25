# Legacy S3N Under Storage

Run Integration Tests
```bash
mvn test -DtestS3Bucket=s3n://<bucket-name>/<folder> -Dfs.s3n.awsAccessKeyId=<access-key> -Dfs.s3n.awsSecretAccessKey=<secret-key>
```
