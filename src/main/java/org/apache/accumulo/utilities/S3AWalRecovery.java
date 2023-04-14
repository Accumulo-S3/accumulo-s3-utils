/*
 * Unlicensed
 */
package org.apache.accumulo.utilities;

import java.io.File;
import java.io.IOException;
import java.net.Inet4Address;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.AbstractMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.AbortMultipartUploadRequest;
import com.amazonaws.services.s3.model.ListMultipartUploadsRequest;
import com.amazonaws.services.s3.model.MultipartUpload;
import com.amazonaws.services.s3.model.UploadPartRequest;
import com.amazonaws.util.AwsHostNameUtils;

public class S3AWalRecovery {
    private static final Logger LOG = LoggerFactory.getLogger(S3AWalRecovery.class);
    private static final String ESCAPED_FORWARD_SLASH = "EFS";
    private static final String ESCAPED_BACKWARD_SLASH = "EBS";
    private static final Pattern PATTERN = Pattern.compile("^s3ablock-(\\d{4})-(.*?)-\\d+\\.tmp$");
    private static final int PATTERN_PART_NUMBER_GROUP = 1;
    private static final int PATTERN_KEY_GROUP = 2;
    private static final String PART_ONE_BUFFERED_FILE_NAME_PREFIX = "s3ablock-0001";
    private static final int PART_NUMBER_PREFIX_LENGTH = PART_ONE_BUFFERED_FILE_NAME_PREFIX.length();
    private String bucketName;
    private String accumuloS3WalPrefix;
    private File s3aBufferDir;
    private AmazonS3 s3client;

    public static void main(String[] args) throws IOException {
        if(isRunning()) {
            LOG.error("The recovery process can't run while the tablet server is running. It will interfere with " +
                    "the tablet server's active processing.");
            System.exit(-1);
        }
        if(args.length != 4) {
            LOG.error("Invalid number of arguments. The recovery tool requires the following arguments in order:\n" +
                    "1. The S3 endpoint URL\n" +
                    "2. The S3 bucket name\n" +
                    "3. The s3a buffer directory (/tmp/hadoop-${user})\n" +
                    "4. The directory/prefix in S3 where write ahead logs are written to by accumulo (accumulo-wal/wal/)");
            System.exit(-1);
        }
        int i = 0;
        String endpointUrl = args[i++];
        String bucketName = args[i++];
        String s3aBufferDir = args[i++];
        String accumuloS3WalPrefix = args[i];

        DefaultAWSCredentialsProviderChain defaultAWSCredentialsProviderChain = new DefaultAWSCredentialsProviderChain();
        AwsClientBuilder.EndpointConfiguration epc = new AwsClientBuilder.EndpointConfiguration(endpointUrl, AwsHostNameUtils.parseRegion(endpointUrl, AmazonS3Client.S3_SERVICE_NAME));
        AmazonS3 client = AmazonS3ClientBuilder
                .standard()
                .withEndpointConfiguration(epc)
                .withCredentials(defaultAWSCredentialsProviderChain).build();

        new S3AWalRecovery(client, bucketName, s3aBufferDir, accumuloS3WalPrefix).run();
    }

    public S3AWalRecovery(AmazonS3 s3client, String bucketName, String s3aBufferDir, String accumuloS3WalPrefix) throws IOException {
        if(s3client == null ||bucketName == null || s3aBufferDir == null || accumuloS3WalPrefix == null) {
            throw new IOException("Illegal constructor argument. Null is not allowed for any arguments");
        }

        this.s3aBufferDir = new File(s3aBufferDir);
        if(!this.s3aBufferDir.isDirectory() || !this.s3aBufferDir.exists()) {
            throw new IOException(String.format("The buffer directory [{}] does not exists or is a file", this.s3aBufferDir.getAbsolutePath()));
        }

        this.s3client = s3client;
        this.bucketName = bucketName;
        this.accumuloS3WalPrefix = accumuloS3WalPrefix;
    }

    public void run() {
        // search the buffer directory for files to flush
        File[] s3aBufferedFiles = s3aBufferDir.listFiles(File::isFile);
        assert s3aBufferedFiles != null;

        // if the buffer directory is empty there's nothing to recover
        if(s3aBufferedFiles.length < 1) {
            LOG.info("There are no buffered files in {}. Nothing to recover.", s3aBufferDir.getAbsolutePath());
            System.exit(0);
        }

        // Process any part one files in the buffered directory
        Stream.of(s3aBufferedFiles)
                .filter(S3AWalRecovery::isPartOneFile)
                .forEach(file -> {
                    try {
                        processPartOneFiles(file);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                });

        // Process any multipart files in the buffered directory
        Map<String, MultipartUpload> mpus = s3client
                .listMultipartUploads(new ListMultipartUploadsRequest(bucketName))
                .getMultipartUploads()
                .stream()
                .map(m -> new AbstractMap.SimpleEntry<>(m.getKey(), m))
                .collect(Collectors.toMap(AbstractMap.SimpleEntry::getKey, AbstractMap.SimpleEntry::getValue));
        Stream.of(s3aBufferedFiles)
                .filter(file -> !isPartOneFile(file))
                .forEach(file -> {
                    try {
                        MultipartUpload mpu = mpus.get(
                                getKeyFromBufferFileName(file.getName())
                        );
                        processMultipartUploads(file, mpu);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                });

        LOG.info("Recovery process has completed successfully.");
    }

    /**
     * This is a comment
     * @param file of the buffered part-1 file to upload to S3
     * @throws IOException when the input file cannot be processed
     */
    protected void processPartOneFiles(File file) throws IOException {
        checkFile(file, false);

        String key = getKeyFromBufferFileName(file.getName());
        // if the key matches the WAL prefix then it's a buffered WAL for this tserver that we should put in the bucket
        if(key.startsWith(getWalPrefix())) {
            LOG.info("Buffered file [{}] matches write ahead log file [{}]. Putting the object directly since it's not part " +
                    "of a multipart upload", file, key);
            s3client.putObject(bucketName, key, file);
        } else if(key.endsWith(".rf_tmp")) {
            LOG.info("Buffered file [{}] matches a temporary r file [{}]. It looks like the tserver died during a compaction." +
                    " The manager will restart the compaction, so we'll delete the buffered file.", file, key);
        } else {
            throw new IOException(String.format("Unsure how to handle part 1 buffered file [%s]. The S3 key [%s] doesn't " +
                    "appear to be a temporary R file, or match the WAL prefix [%s]", file, key, getWalPrefix()));
        }

        if(!file.delete()) {
            throw new IOException(String.format("Unable to delete file %s", file.getName()));
        }
    }

    protected void processMultipartUploads(File file, MultipartUpload mpu) throws IOException {
        checkFile(file, true);

        String key = mpu.getKey();
        String uploadID = mpu.getUploadId();

        // if the key matches the WAL prefix then it's a buffered WAL for this tserver that we should put in the bucket
        if(key.startsWith(getWalPrefix())) {
            LOG.info("Buffered file [{}] matches write ahead log file [{}]. Putting the object directly since it's not part " +
                    "of a multipart upload", file, key);

            int finalPartNumber = getPartNumberFromFile(file.getName());
            UploadPartRequest upr = new UploadPartRequest()
                    .withBucketName(bucketName)
                    .withKey(key)
                    .withFile(file)
                    .withPartNumber(finalPartNumber)
                    .withLastPart(true);
            s3client.uploadPart(upr);
                    } else if(key.endsWith(".rf_tmp")) {
            LOG.info("Buffered file [{}] matches a temporary r file [{}]. It looks like the tserver died during a compation." +
                    " The manager will restart the compaction, so we'll abort the multi part upload and delete the buffered file.", file, key);
            AbortMultipartUploadRequest abortRequest = new AbortMultipartUploadRequest(bucketName, key, uploadID);
            s3client.abortMultipartUpload(abortRequest);
                    } else {
            throw new IOException(String.format("Unsure how to handle multipart buffered file [%s]", file));
        }

        if(!file.delete()) {
            throw new IOException(String.format("Unable to delete file %s", file.getName()));
        }
    }

    protected static String getKeyFromBufferFileName(String fileName) throws IOException {
        String key = regexMatch(fileName).group(PATTERN_KEY_GROUP);
        return key
                .replace(ESCAPED_BACKWARD_SLASH, "\\")
                .replace(ESCAPED_FORWARD_SLASH, "/");
    }

    protected static boolean isPartOneFile(File file) {
        if(file.getName().substring(0, PART_NUMBER_PREFIX_LENGTH).equals(PART_ONE_BUFFERED_FILE_NAME_PREFIX)) {
            return true;
        }
        return false;
    }

    protected static int getPartNumberFromFile(String fileName) throws IOException {
        return Integer.parseInt(regexMatch(fileName).group(PATTERN_PART_NUMBER_GROUP));
    }

    protected static Matcher regexMatch(String fileName) throws IOException {
        Matcher matcher = PATTERN.matcher(fileName);
        if(matcher.find()) {
            return matcher;
        } else {
            throw new IOException("The file name doesn't match expected input");
        }
    }

    protected String getWalPrefix() throws UnknownHostException {
        if(!accumuloS3WalPrefix.endsWith("/")) {
            return String.format("%s/%s+9997/", accumuloS3WalPrefix, Inet4Address.getLocalHost().getHostName());
        } else {
            return String.format("%s%s+9997/", accumuloS3WalPrefix, Inet4Address.getLocalHost().getHostName());
        }
    }

    protected static boolean isRunning() {
        try {
            Socket socket = new Socket(Inet4Address.getLocalHost(), 9997);
            socket.close();
            return true;
        } catch (IOException e) {
            LOG.info("Service is not listening on port " + 9997);
        }

        return false;
    }

    private static void checkFile(File file, boolean multipart) throws IOException {
        boolean isValid = true;
        StringBuilder sb = new StringBuilder();

        if(!file.exists()) {
            isValid = false;
            sb.append(String.format("No such file exists [{}].\n", file.getAbsolutePath()));
        }
        if(file.isDirectory()) {
            isValid = false;
            sb.append(String.format("Invalid argument. The argument [{}] is a directory.\n", file.getAbsolutePath()));
        }
        if(!multipart && !isPartOneFile(file)) {
            isValid = false;
            sb.append(String.format("The file is part of a multipart upload: [{}]\n", file));
        }
        if(multipart && isPartOneFile(file)) {
            isValid = false;
            sb.append(String.format("The file isn't part of a multipart upload: [{}]\n", file));
        }

        if(!isValid) {
            throw new IOException(sb.toString());
        }
    }
}
