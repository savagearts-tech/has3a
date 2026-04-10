package io.github.has3a;

import org.apache.iceberg.aws.s3.S3FileIOProperties;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.Objects;

/**
 * Configures Iceberg {@link org.apache.iceberg.aws.s3.S3FileIO} local upload staging so it does not
 * default to {@code java.io.tmpdir} (often the OS / system disk).
 *
 * <p>Call {@link #applyPreferredStagingDirectory(Map)} on your catalog property map before
 * {@code Catalog.initialize(...)}. Iceberg reads {@link S3FileIOProperties#STAGING_DIRECTORY} from
 * those properties.
 *
 * <p>Resolution order when {@code s3.staging-directory} is not already set:
 * <ol>
 *   <li>Catalog property {@value #HAS3A_STAGING_CATALOG_KEY}</li>
 *   <li>System property {@value #HAS3A_STAGING_SYS_PROP}</li>
 *   <li>Environment variable {@value #HAS3A_STAGING_ENV}</li>
 * </ol>
 */
public final class IcebergS3FileIOStaging {

    /**
     * Optional catalog key mirroring {@link S3FileIOProperties#STAGING_DIRECTORY} for operators
     * who colocate has3a-related settings.
     */
    public static final String HAS3A_STAGING_CATALOG_KEY = "has3a.s3.staging-directory";

    /** JVM system property for staging path (same semantics as {@value #HAS3A_STAGING_CATALOG_KEY}). */
    public static final String HAS3A_STAGING_SYS_PROP = "has3a.s3.staging-directory";

    /** Environment variable for staging path. */
    public static final String HAS3A_STAGING_ENV = "HAS3A_S3_STAGING_DIRECTORY";

    private IcebergS3FileIOStaging() {}

    /**
     * Ensures {@link S3FileIOProperties#STAGING_DIRECTORY} is set to a fast data volume when the
     * caller provides a path via has3a keys or the environment.
     *
     * <p>If {@code s3.staging-directory} is already non-blank in {@code catalogProperties}, this
     * method does nothing. Otherwise the first non-blank value from the resolution order is
     * normalized to an absolute path, created on disk if missing, and stored under the Iceberg key.
     *
     * @param catalogProperties mutable Iceberg catalog configuration map
     */
    public static void applyPreferredStagingDirectory(Map<String, String> catalogProperties) {
        Objects.requireNonNull(catalogProperties, "catalogProperties");
        String icebergKey = S3FileIOProperties.STAGING_DIRECTORY;
        if (isNonBlank(catalogProperties.get(icebergKey))) {
            return;
        }
        String raw = firstNonBlank(
                catalogProperties.get(HAS3A_STAGING_CATALOG_KEY),
                System.getProperty(HAS3A_STAGING_SYS_PROP),
                System.getenv(HAS3A_STAGING_ENV));
        if (!isNonBlank(raw)) {
            return;
        }
        Path dir = Paths.get(raw.trim()).toAbsolutePath().normalize();
        try {
            Files.createDirectories(dir);
        } catch (Exception e) {
            throw new IllegalStateException(
                    "has3a: cannot create S3FileIO staging directory: " + dir, e);
        }
        catalogProperties.put(icebergKey, dir.toString());
    }

    private static boolean isNonBlank(String s) {
        return s != null && !s.isBlank();
    }

    private static String firstNonBlank(String a, String b, String c) {
        if (isNonBlank(a)) {
            return a;
        }
        if (isNonBlank(b)) {
            return b;
        }
        if (isNonBlank(c)) {
            return c;
        }
        return null;
    }
}
