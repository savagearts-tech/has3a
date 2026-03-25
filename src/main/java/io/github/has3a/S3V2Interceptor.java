package io.github.has3a;

import software.amazon.awssdk.core.interceptor.Context;
import software.amazon.awssdk.core.interceptor.ExecutionAttributes;
import software.amazon.awssdk.core.interceptor.ExecutionInterceptor;
import software.amazon.awssdk.core.interceptor.SdkExecutionAttribute;

import java.net.URI;

/**
 * Optional execution interceptor that rewrites the endpoint of an in-flight S3 request
 * by reading a per-request {@link #TARGET_ENDPOINT} attribute.
 *
 * <p><strong>�?Internal API warning:</strong> {@link SdkExecutionAttribute#CLIENT_ENDPOINT}
 * is an internal AWS SDK attribute (not part of the public API contract). It works as expected
 * with SDK 2.20.x but may change without notice in future versions.
 *
 * <p><strong>Migration path:</strong> AWS SDK v2 2.21+ supports a pluggable
 * {@code EndpointProvider} via {@code S3Client.builder().endpointProvider(...)}.
 * Migrate to that mechanism when upgrading past 2.20.x to avoid relying on internal attributes.
 *
 * <p>This interceptor is currently unused in the hot path (BulkheadS3ClientFactory routes
 * by instantiating per-endpoint S3Clients). It is retained for future dynamic-routing use cases.
 */
public class S3V2Interceptor implements ExecutionInterceptor {

    public static final software.amazon.awssdk.core.interceptor.ExecutionAttribute<URI> TARGET_ENDPOINT =
            new software.amazon.awssdk.core.interceptor.ExecutionAttribute<>("TARGET_ENDPOINT");

    @Override
    public void beforeExecution(Context.BeforeExecution context, ExecutionAttributes executionAttributes) {
        URI dynamicEndpoint = executionAttributes.getAttribute(TARGET_ENDPOINT);
        if (dynamicEndpoint != null) {
            executionAttributes.putAttribute(SdkExecutionAttribute.ENDPOINT_OVERRIDDEN, true);
            executionAttributes.putAttribute(SdkExecutionAttribute.CLIENT_ENDPOINT, dynamicEndpoint);
        }
    }
}
