package io.github.has3a;

import org.apache.iceberg.aws.s3.S3FileIOProperties;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class IcebergS3FileIOStagingTest {

    @Test
    void should_PreserveExplicitIcebergStaging_When_AlreadySet(@TempDir Path tmp) throws Exception {
        Path explicit = tmp.resolve("explicit");
        Files.createDirectories(explicit);

        Map<String, String> props = new HashMap<>();
        props.put(S3FileIOProperties.STAGING_DIRECTORY, explicit.toString());
        props.put(IcebergS3FileIOStaging.HAS3A_STAGING_CATALOG_KEY, tmp.resolve("ignored").toString());

        IcebergS3FileIOStaging.applyPreferredStagingDirectory(props);

        assertEquals(explicit.toString(), props.get(S3FileIOProperties.STAGING_DIRECTORY));
    }

    @Test
    void should_MapHas3aCatalogKeyToIcebergStaging_When_IcebergKeyAbsent(@TempDir Path tmp) throws Exception {
        Path staging = tmp.resolve("via-has3a-key");
        Map<String, String> props = new HashMap<>();
        props.put(IcebergS3FileIOStaging.HAS3A_STAGING_CATALOG_KEY, staging.toString());

        IcebergS3FileIOStaging.applyPreferredStagingDirectory(props);

        assertEquals(staging.toAbsolutePath().normalize().toString(),
                props.get(S3FileIOProperties.STAGING_DIRECTORY));
        assertTrue(Files.isDirectory(staging));
    }

    @Test
    void should_UseSystemProperty_When_NoCatalogKeys(@TempDir Path tmp) {
        Path staging = tmp.resolve("via-sysprop");
        String previous = System.setProperty(IcebergS3FileIOStaging.HAS3A_STAGING_SYS_PROP, staging.toString());
        try {
            Map<String, String> props = new HashMap<>();
            IcebergS3FileIOStaging.applyPreferredStagingDirectory(props);

            assertEquals(staging.toAbsolutePath().normalize().toString(),
                    props.get(S3FileIOProperties.STAGING_DIRECTORY));
            assertTrue(Files.isDirectory(staging));
        } finally {
            if (previous == null) {
                System.clearProperty(IcebergS3FileIOStaging.HAS3A_STAGING_SYS_PROP);
            } else {
                System.setProperty(IcebergS3FileIOStaging.HAS3A_STAGING_SYS_PROP, previous);
            }
        }
    }

    @Test
    void should_LeaveMapUnchanged_When_NoStagingConfigured() {
        Map<String, String> props = new HashMap<>();
        props.put("other", "x");
        IcebergS3FileIOStaging.applyPreferredStagingDirectory(props);
        assertEquals(1, props.size());
        assertEquals("x", props.get("other"));
    }
}
