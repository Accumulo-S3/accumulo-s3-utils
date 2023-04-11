/*
 *
 */
/*
 * Unlicensed
 */
package org.apache.accumulo.utilities;

import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.eq;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.net.Inet4Address;
import java.net.UnknownHostException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;

import org.easymock.EasyMock;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.MultipartUpload;

public class S3AWalRecoveryTest {
    private static final String BUCKET_NAME = "bucket";
    private static final String BUFFER_DIR = "src/test/resources/s3a_dir";
    private static final String WAL_PREFIX = "accumulo-wal/wal/";
    private static final String TABLE_RFILE_PREFIX = "accumulo/accumulo/tables";
    private static final String WAL1_PART1_NAME;
    private static final String WAL1_PART2_NAME;
    private static final String WAL1_PART12_NAME;
    private static final String WAL2_PART1_NAME;
    private static final String WAL2_PART2_NAME;
    private static final String BLOCK1_COMPACTION_FILE_NAME = "s3ablock-0001-accumuloEFSaccumuloEFStablesEFS+rEFSroot_tabletEFSA0000005.rf_tmp-5585275724905231424.tmp";
    private static final String BLOCK2_COMPACTION_FILE_NAME = "s3ablock-0002-accumuloEFSaccumuloEFStablesEFS+rEFSroot_tabletEFSA0000005.rf_tmp-5585275724905231424.tmp";
    private static final String WAL1_S3_KEY;
    private static final String WAL2_S3_KEY;
    private static final String COMPACTION_S3_KEY = "accumulo/accumulo/tables/+r/root_tablet/A0000005.rf_tmp";
    private static final String MOCK_UPLOAD_ID = "abc123";

    static {
        try {
            WAL1_PART1_NAME = String.format(
                    "s3ablock-0001-accumulo-walEFSwalEFS%s+9997EFSe2823e96-6e51-4657-b912-840c65f36a9a-5585275724905231424.tmp",
                    Inet4Address.getLocalHost().getHostName());
            WAL1_PART2_NAME = String.format(
                    "s3ablock-0002-accumulo-walEFSwalEFS%s+9997EFSe2823e96-6e51-4657-b912-840c65f36a9a-5585275724905231424.tmp",
                    Inet4Address.getLocalHost().getHostName());
            WAL1_PART12_NAME = String.format(
                    "s3ablock-0012-accumulo-walEFSwalEFS%s+9997EFSe2823e96-6e51-4657-b912-840c65f36a9a-5585275724905231424.tmp",
                    Inet4Address.getLocalHost().getHostName());
            WAL2_PART1_NAME = String.format(
                    "s3ablock-0001-accumulo-walEFSwalEFS%s+9997EFSe6d6f4c9-1bc7-4517-b3c7-362c0dfa1bbd-8653103153730461698.tmp",
                    Inet4Address.getLocalHost().getHostName());
            WAL2_PART2_NAME = String.format(
                    "s3ablock-0002-accumulo-walEFSwalEFS%s+9997EFSe6d6f4c9-1bc7-4517-b3c7-362c0dfa1bbd-8653103153730461698.tmp",
                    Inet4Address.getLocalHost().getHostName());
            WAL1_S3_KEY = String.format(
                    "accumulo-wal/wal/%s+9997/e2823e96-6e51-4657-b912-840c65f36a9a",
                    Inet4Address.getLocalHost().getHostName());
            WAL2_S3_KEY = String.format(
                    "accumulo-wal/wal/%s+9997/e6d6f4c9-1bc7-4517-b3c7-362c0dfa1bbd",
                    Inet4Address.getLocalHost().getHostName());
        } catch (UnknownHostException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testConstructorFailsForNullArguments() throws IOException {
        AmazonS3 client = EasyMock.createMock(AmazonS3.class);
        assertThrows(IOException.class, () -> {
            new S3AWalRecovery(null, BUCKET_NAME, BUFFER_DIR, WAL_PREFIX);
        });
        assertThrows(IOException.class, () -> {
                new S3AWalRecovery(client, null, BUFFER_DIR, WAL_PREFIX);
        });
        assertThrows(IOException.class, () -> {
            new S3AWalRecovery(client, BUCKET_NAME, null, WAL_PREFIX);
        });
        assertThrows(IOException.class, () -> {
            new S3AWalRecovery(client, BUCKET_NAME, BUFFER_DIR, null);
        });
        assertThrows(IOException.class, () -> {
            new S3AWalRecovery(client, BUCKET_NAME, "src/test/resources_dir_doesnt_exist", WAL_PREFIX);
        });
    }

    @Test
    public void isPartOneFileTest() {
        assertTrue(S3AWalRecovery.isPartOneFile(Path.of(BUFFER_DIR, WAL1_PART1_NAME).toFile()));
        assertFalse(S3AWalRecovery.isPartOneFile(Path.of(BUFFER_DIR, WAL1_PART2_NAME).toFile()));
        assertFalse(S3AWalRecovery.isPartOneFile(Path.of(BUFFER_DIR, WAL1_PART12_NAME).toFile()));

        assertTrue(S3AWalRecovery.isPartOneFile(Path.of(BUFFER_DIR, WAL2_PART1_NAME).toFile()));
        assertFalse(S3AWalRecovery.isPartOneFile(Path.of(BUFFER_DIR, WAL2_PART2_NAME).toFile()));
    }

    @Test
    public void getPartNumberFromFileTest() throws IOException {
        assertEquals(1, S3AWalRecovery.getPartNumberFromFile(WAL1_PART1_NAME));
        assertEquals(2, S3AWalRecovery.getPartNumberFromFile(WAL1_PART2_NAME));
        assertEquals(12, S3AWalRecovery.getPartNumberFromFile(WAL1_PART12_NAME));

        assertEquals(1, S3AWalRecovery.getPartNumberFromFile(WAL2_PART1_NAME));
        assertEquals(2, S3AWalRecovery.getPartNumberFromFile(WAL2_PART2_NAME));
    }

    @Test
    public void getKeyFromBufferFileNameTest() throws IOException {
        assertEquals(WAL1_S3_KEY, S3AWalRecovery.getKeyFromBufferFileName(WAL1_PART1_NAME));
        assertEquals(WAL1_S3_KEY, S3AWalRecovery.getKeyFromBufferFileName(WAL1_PART2_NAME));
        assertEquals(WAL1_S3_KEY, S3AWalRecovery.getKeyFromBufferFileName(WAL1_PART12_NAME));

        assertEquals(WAL2_S3_KEY, S3AWalRecovery.getKeyFromBufferFileName(WAL2_PART1_NAME));
        assertEquals(WAL2_S3_KEY, S3AWalRecovery.getKeyFromBufferFileName(WAL2_PART2_NAME));
    }

    @Test
    public void processPartOneFilesTest() throws IOException {
        AmazonS3 client;
        S3AWalRecovery s3AWalRecovery;
        File testFile;

        // should put the object and remove the local file
        client = EasyMock.createMock(AmazonS3.class);
        testFile = Path.of(BUFFER_DIR, WAL1_PART1_NAME).toFile();
        // expect the client to call putObject with the correct parameters
        expect(client.putObject(eq(BUCKET_NAME), eq(WAL1_S3_KEY), eq(testFile))).andReturn(null);
        replay(client);
        s3AWalRecovery = new S3AWalRecovery(client, BUCKET_NAME, BUFFER_DIR, WAL_PREFIX);
        s3AWalRecovery.processPartOneFiles(testFile);
        // expect the WAL1 file to have been deleted
        assertFalse(Files.exists(Path.of(BUFFER_DIR, WAL1_PART1_NAME)));
        verify(client);

        // should put the object and remove the local file
        client = EasyMock.createMock(AmazonS3.class);
        testFile = Path.of(BUFFER_DIR, WAL2_PART1_NAME).toFile();
        // expect the client to call putObject with the correct parameters
        expect(client.putObject(eq(BUCKET_NAME), eq(WAL2_S3_KEY), eq(testFile))).andReturn(null);
        replay(client);
        s3AWalRecovery = new S3AWalRecovery(client, BUCKET_NAME, BUFFER_DIR, WAL_PREFIX);
        s3AWalRecovery.processPartOneFiles(testFile);
        // expect the WAL2 file to have been deleted
        assertFalse(Files.exists(Path.of(BUFFER_DIR, WAL2_PART1_NAME)));
        verify(client);

        // calling processPartOneFiles on a part 2 file should throw and IOException and the local file shouldn't be removed
        assertThrows(IOException.class, () -> {
            new S3AWalRecovery(EasyMock.createMock(AmazonS3.class), BUCKET_NAME, BUFFER_DIR, WAL_PREFIX)
                    .processPartOneFiles(Path.of(BUFFER_DIR, WAL1_PART2_NAME).toFile());
        });
        assertTrue(Files.exists(Path.of(BUFFER_DIR, WAL1_PART2_NAME)));

        // calling processPartOneFiles on a part 2 file should throw and IOException and the local file shouldn't be removed
        assertThrows(IOException.class, () -> {
            new S3AWalRecovery(EasyMock.createMock(AmazonS3.class), BUCKET_NAME, BUFFER_DIR, WAL_PREFIX)
                    .processPartOneFiles(Path.of(BUFFER_DIR, WAL2_PART2_NAME).toFile());
        });
        assertTrue(Files.exists(Path.of(BUFFER_DIR, WAL2_PART2_NAME)));

        // expected partially finished compaction files to be deleted locally and not to be uploaded to S3
        client = EasyMock.createStrictMock(AmazonS3.class);
        replay(client); // expect no calls made to the mock
        testFile = Path.of(BUFFER_DIR, BLOCK1_COMPACTION_FILE_NAME).toFile();
        s3AWalRecovery = new S3AWalRecovery(client, BUCKET_NAME, BUFFER_DIR, TABLE_RFILE_PREFIX);
        s3AWalRecovery.processPartOneFiles(testFile);
        // expect the WAL1 file to have been deleted
        assertFalse(Files.exists(Path.of(BUFFER_DIR, BLOCK1_COMPACTION_FILE_NAME)));
        verify(client);

        // calling processPartOneFiles on a part 2 file should throw and IOException and the local file shouldn't be removed
        assertThrows(IOException.class, () -> {
            new S3AWalRecovery(EasyMock.createMock(AmazonS3.class), BUCKET_NAME, BUFFER_DIR, TABLE_RFILE_PREFIX)
                    .processPartOneFiles(Path.of(BUFFER_DIR, BLOCK1_COMPACTION_FILE_NAME).toFile());
        });
        assertTrue(Files.exists(Path.of(BUFFER_DIR, BLOCK2_COMPACTION_FILE_NAME)));
    }

    @Test
    public void testProcessMultipartFiles() throws IOException {
        AmazonS3 client;
        S3AWalRecovery s3AWalRecovery;
        File testFile;
        MultipartUpload mpu;

        // calling processMultipartUploads on a part 1 file should throw and IOException and the local file shouldn't be removed
        assertThrows(IOException.class, () -> {
            new S3AWalRecovery(EasyMock.createMock(AmazonS3.class), BUCKET_NAME, BUFFER_DIR, TABLE_RFILE_PREFIX)
                    .processMultipartUploads(Path.of(BUFFER_DIR, BLOCK1_COMPACTION_FILE_NAME).toFile(), EasyMock.createMock(MultipartUpload.class));
        });
        assertTrue(Files.exists(Path.of(BUFFER_DIR, BLOCK1_COMPACTION_FILE_NAME)));

        // the utility doesn't finish partial uploads of compaction files since the manager reschedules the compaction on failure
        // expect the multipart upload to have been aborted, and for the local file to have been deleted
        testFile = Path.of(BUFFER_DIR, BLOCK2_COMPACTION_FILE_NAME).toFile();
        client = EasyMock.createStrictMock(AmazonS3.class);
        mpu = EasyMock.createStrictMock(MultipartUpload.class);
        expect(mpu.getKey()).andReturn(COMPACTION_S3_KEY);
        expect(mpu.getUploadId()).andReturn(MOCK_UPLOAD_ID);
        client.abortMultipartUpload(anyObject());
        expectLastCall();
        replay(client, mpu); // expect no calls made to the mock
        s3AWalRecovery = new S3AWalRecovery(client, BUCKET_NAME, BUFFER_DIR, WAL_PREFIX);
        s3AWalRecovery.processMultipartUploads(testFile, mpu);
        // expect the tmp_rf file to have been deleted
        assertFalse(Files.exists(testFile.toPath()));
        verify(client);

        // calling processMultipartUploads on a part 1 file should throw and IOException and the local file shouldn't be removed
        assertThrows(IOException.class, () -> {
            new S3AWalRecovery(EasyMock.createMock(AmazonS3.class), BUCKET_NAME, BUFFER_DIR, WAL_PREFIX)
                    .processMultipartUploads(Path.of(BUFFER_DIR, WAL1_PART1_NAME).toFile(), EasyMock.createMock(MultipartUpload.class));
        });
        assertTrue(Files.exists(Path.of(BUFFER_DIR, WAL1_PART1_NAME)));

        // the utility doesn't finish partial uploads of compaction files since the manager reschedules the compaction on failure
        // expect the multipart upload to have been aborted, and for the local file to have been deleted
        testFile = Path.of(BUFFER_DIR, WAL1_PART2_NAME).toFile();
        client = EasyMock.createStrictMock(AmazonS3.class);
        mpu = EasyMock.createStrictMock(MultipartUpload.class);
        expect(mpu.getKey()).andReturn(WAL1_S3_KEY);
        expect(mpu.getUploadId()).andReturn(MOCK_UPLOAD_ID);
        expect(client.uploadPart(anyObject())).andReturn(null);
        replay(client, mpu); // expect no calls made to the mock
        s3AWalRecovery = new S3AWalRecovery(client, BUCKET_NAME, BUFFER_DIR, WAL_PREFIX);
        s3AWalRecovery.processMultipartUploads(testFile, mpu);
        // expect the uploaded file to have been deleted locally
        assertFalse(Files.exists(testFile.toPath()));
        verify(client);
    }

    @Before
    public void generateTestFiles() throws IOException {
        Files.createFile(Path.of(BUFFER_DIR, WAL1_PART1_NAME));
        Files.createFile(Path.of(BUFFER_DIR, WAL1_PART2_NAME));
        Files.createFile(Path.of(BUFFER_DIR, WAL1_PART12_NAME));
        Files.createFile(Path.of(BUFFER_DIR, WAL2_PART1_NAME));
        Files.createFile(Path.of(BUFFER_DIR, WAL2_PART2_NAME));
        Files.createFile(Path.of(BUFFER_DIR, BLOCK1_COMPACTION_FILE_NAME));
        Files.createFile(Path.of(BUFFER_DIR, BLOCK2_COMPACTION_FILE_NAME));
    }

    @After
    public void cleanup() throws IOException {
        File bufferDir = new File(BUFFER_DIR);
        for(File f : Objects.requireNonNull(bufferDir.listFiles(File::isFile))) {
            f.delete();
        }
    }
}
