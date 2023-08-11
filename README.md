# accumulo-s3-utils
### Helpful tools for running Accumulo on S3 and in kubernetes

#
#### S3AWalRecovery 
Rather than uploading data to S3 when flush() is called on the S3AFileOutputStreams, the stream flushes data to a local buffer. By default the buffer is stored on disk in /tmp/hadoop-$user/s3a. This can cause issues with the write-ahead-log recovery process after a tserver becomes unresponsive. This utility is meant to help accumulo tservers that are running with the hadoop-aws S3AFileSystem recover after locally buffered data. Write-ahead-log files are uploaded to S3 and any temporary compaction uploads are aborted and deleted locally. 

#### Execute the command below with aws-java-sdk-s3 on your classpath
``java org.apache.accumulo.utilities.S3AWalRecovery $ENDPOINT_URL $BUCKET_NAME $S3A_BUFFER_DIR S3_WAL_PREFIX``

## Exporting to codeartifact
1. Get the auth token for the domain `aws codeartifact get-authorization-token --domain focusedleap`
2. Set the CODEARTIFACT_AUTH_TOKEN environment variable to the authorizationToken value
3. Run the deploy command `mvn deploy -DrepositoryId=codeartifact`
   * To overwrite a previous verion you must first delete the package version: `aws codeartifact delete-package-versions --repository "accumulo-s3-utils" --domain "focusedleap" --format "maven" --namespace "r62" --package "accumulo-s3-utils" --versions "VERSION"`