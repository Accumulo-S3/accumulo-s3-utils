package org.apache.accumulo.utilities;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.Protocol;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.util.AwsHostNameUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * This class is a utility to be used in accumulo pods running in kubernetes with S3 as the underlying
 * file system. The utility search S3 for a previously initialized accumulo database to inform an
 * accumulo deployment of whether a deployment already exists. If the force flag is set to true,
 * this utility will delete the objects in S3 so that a new database can be initialized.
 */
public class PrepBucketForInit {
  private static final Logger LOG = LoggerFactory.getLogger(PrepBucketForInit.class);
  private static final String ACCUMULO_DB_PREFIX = "accumulo/";
  private static final String ACCUMULO_WAL_PREFIX = "accumulo-wal/";
  private static final Path lockDir = Path.of("/tmp/accumulo_bucket_objects");
  private static int objects_in_bucket = 0;

  public static void main(String[] args) throws IOException {
    if(args.length != 5) {
      LOG.error("Invalid number of arguments. The recovery tool requires the following arguments in order:\n" +
        "1. The S3 endpoint URL\n" +
        "2. The S3 bucket name\n" +
        "3. Whether or not to delete existing data if it exists to prepare for a new database to be initialize\n" +
        "4. SSL enabled.\n" +
        "5. Path style access.");
      System.exit(-1);
    }
    int i = 0;
    String endpointUrl = args[i++];
    String bucketName = args[i++];
    boolean forceDelete = Boolean.parseBoolean(args[i++]);
    boolean sslEnabled = Boolean.parseBoolean(args[i++]);
    boolean pathStyleAccess = Boolean.parseBoolean(args[i]);

    endpointUrl = addProtocol(endpointUrl, sslEnabled);

    DefaultAWSCredentialsProviderChain defaultAWSCredentialsProviderChain = new DefaultAWSCredentialsProviderChain();
    AwsClientBuilder.EndpointConfiguration epc = new AwsClientBuilder.EndpointConfiguration(endpointUrl, AwsHostNameUtils.parseRegion(endpointUrl, AmazonS3Client.S3_SERVICE_NAME));
    ClientConfiguration clientConfig = new ClientConfiguration();
    if(sslEnabled) {
      clientConfig.setProtocol(Protocol.HTTPS);
    } else {
      clientConfig.setProtocol(Protocol.HTTP);
    }
    AmazonS3 client = AmazonS3ClientBuilder
      .standard()
      .withEndpointConfiguration(epc)
      .withPathStyleAccessEnabled(pathStyleAccess)
      .withCredentials(defaultAWSCredentialsProviderChain).build();

    Files.deleteIfExists(lockDir);
    checkPrefix(client, bucketName, ACCUMULO_DB_PREFIX, forceDelete);
    checkPrefix(client, bucketName, ACCUMULO_WAL_PREFIX, forceDelete);
    outputObjectCount();
  }

  private static void checkPrefix(AmazonS3 client, String bucket, String prefix, boolean forceDel) {
    ListObjectsV2Request listRequest = new ListObjectsV2Request()
      .withBucketName(bucket)
      .withPrefix(prefix);
      ListObjectsV2Result objectListing;
    do {
      objectListing = client.listObjectsV2(listRequest);
      for (S3ObjectSummary objectSummary : objectListing.getObjectSummaries()) {
        if (forceDel) {
          LOG.warn("Deleting previous Accumulo database object [{}]", objectSummary.getKey());
          client.deleteObject(bucket, objectSummary.getKey());
        } else {
          objects_in_bucket++;
          LOG.warn("Existing Accumulo deployment object found [{}]", objectSummary.getKey());
        }
      }
      // Set the continuation token to retrieve the next page of results
      listRequest.setContinuationToken(objectListing.getNextContinuationToken());
    } while (objectListing.isTruncated());
  }

  private static void outputObjectCount() {
    try (FileWriter writer = new FileWriter(lockDir.toString())) {
      LOG.info("Saving total object count [{}] to the lock file [{}]...", objects_in_bucket, lockDir);
      writer.write(Integer.toString(objects_in_bucket));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private static String addProtocol(String endpointURL, boolean sslEnabled) {
    String url;
    if(!endpointURL.toLowerCase().startsWith("http")) {
      if (sslEnabled) {
        url = "https://" + endpointURL;
      } else {
        url = "http://" + endpointURL;
      }
    } else {
      url = endpointURL;
    }

    LOG.info("Using endpoint URL [{}]", url);
    return url;
  }
}
