package dev.restate.examples.azure;

import com.microsoft.azure.functions.*;
import com.microsoft.azure.functions.annotation.AuthorizationLevel;
import com.microsoft.azure.functions.annotation.FunctionName;
import com.microsoft.azure.functions.annotation.HttpTrigger;
import dev.restate.sdk.core.InvocationFlow;
import dev.restate.sdk.core.ProtocolException;
import dev.restate.sdk.core.ResolvedEndpointHandler;
import dev.restate.sdk.core.RestateEndpoint;
import dev.restate.sdk.core.manifest.EndpointManifestSchema;
import dev.restate.sdk.version.Version;
import io.opentelemetry.context.Context;
import org.apache.logging.log4j.ThreadContext;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.nio.charset.StandardCharsets;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Flow;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Pattern;

/**
 * Azure Functions HTTP Trigger to Restate Endpoint adapter.
 */
public class RestateAzureFnAdapter {

    private static final Pattern SLASH = Pattern.compile(Pattern.quote("/"));
    private static final String INVOKE_PATH_SEGMENT = "invoke";
    private static final String DISCOVER_PATH = "/discover";

    private final RestateEndpoint restateEndpoint;

    public RestateAzureFnAdapter() {
        this.restateEndpoint = RestateEndpoint.newBuilder(EndpointManifestSchema.ProtocolMode.REQUEST_RESPONSE)
                // .withRequestIdentityVerifier(...) // TODO: required to protect handlers from unauthorized invocations
                .bind(RestateEndpoint.discoverServiceDefinitionFactory(new GreeterObject()).create(new GreeterObject()), null)
                .build();
    }

    @FunctionName("restate")
    public HttpResponseMessage run(
            @HttpTrigger(name = "restateEndpoint", methods = {HttpMethod.POST}, authLevel = AuthorizationLevel.ANONYMOUS)
            HttpRequestMessage<Optional<String>> request,
            final ExecutionContext context) {

        context.getLogger().info("Delegating request to Restate endpoint: " + request.getUri().getPath());

        String requestPath = request.getUri().getPath();
        String path = requestPath.endsWith("/")
                ? requestPath.substring(0, requestPath.length() - 1)
                : requestPath;

        try {
            if (path.endsWith(DISCOVER_PATH)) {
                return this.handleDiscovery(request);
            }
            return this.handleInvoke(request, context.getLogger());
        } catch (ProtocolException e) {
            // We can handle protocol exceptions by returning back the correct response
            context.getLogger().log(Level.WARNING, "Error when handling the request", e);
            return request.createResponseBuilder(HttpStatus.valueOf(e.getCode()))
                    .header("content-type", "text/plain")
                    .header("x-restate-server", Version.X_RESTATE_SERVER)
                    .body(e.getMessage())
                    .build();
        }
    }

    private HttpResponseMessage handleInvoke(HttpRequestMessage<Optional<String>> request, Logger logger) {
        // Parse request
        String[] pathSegments = SLASH.split(request.getUri().getPath());
        if (pathSegments.length < 3
                || !INVOKE_PATH_SEGMENT.equalsIgnoreCase(pathSegments[pathSegments.length - 3])) {
            logger.log(Level.WARNING, "Path doesn't match the pattern /invoke/SvcName/MethodName: '{}'", new Object[]{request.getUri().getPath()});
            return request.createResponseBuilder(HttpStatus.NOT_FOUND).build();
        }
        String serviceName = pathSegments[pathSegments.length - 2];
        String handlerName = pathSegments[pathSegments.length - 1];

        // Resolve handler
        ResolvedEndpointHandler handler;
        try {
            handler =
                    this.restateEndpoint.resolve(
                            request.getHeaders().get("content-type"),
                            serviceName,
                            handlerName,
                            request.getHeaders()::get,
                            Context.current(), // TODO: propagate OTEL context
                            RestateEndpoint.LoggingContextSetter.THREAD_LOCAL_INSTANCE,
                            null);
        } catch (ProtocolException e) {
            logger.log(Level.WARNING, "Error when resolving the grpc handler", e);
            return request.createResponseBuilder(HttpStatus.valueOf(e.getCode())).build();
        }

        BufferedPublisher publisher = new BufferedPublisher(ByteBuffer.wrap(request.getBody().orElseThrow().getBytes(StandardCharsets.UTF_8)));
        ResultSubscriber subscriber = new ResultSubscriber();

        // Wire handler
        publisher.subscribe(handler);
        handler.subscribe(subscriber);

        // Await the result
        byte[] responseBody;
        try {
            responseBody = subscriber.getResult();
        } catch (Error | RuntimeException e) {
            throw e;
        } catch (Throwable e) {
            throw new RuntimeException(e);
        }

        // Clear logging
        ThreadContext.clearAll();

        return request.createResponseBuilder(HttpStatus.OK)
                .header("content-type", handler.responseContentType())
                .header("x-restate-server", Version.X_RESTATE_SERVER)
                .body(responseBody).build();
    }

    private HttpResponseMessage handleDiscovery(HttpRequestMessage<Optional<String>> request) {
        RestateEndpoint.DiscoveryResponse discoveryResponse =
                this.restateEndpoint.handleDiscoveryRequest(request.getHeaders().get("accept"));

        return request.createResponseBuilder(HttpStatus.OK)
                .header("content-type", discoveryResponse.getContentType())
                .header("x-restate-server", Version.X_RESTATE_SERVER)
                .body(discoveryResponse.getSerializedManifest())
                .build();
    }

    static class ResultSubscriber implements InvocationFlow.InvocationOutputSubscriber {

        private final CompletableFuture<Void> completionFuture;
        private final ByteArrayOutputStream outputStream;
        private final WritableByteChannel channel;

        ResultSubscriber() {
            this.completionFuture = new CompletableFuture<>();
            this.outputStream = new ByteArrayOutputStream();
            this.channel = Channels.newChannel(outputStream);
        }

        @Override
        public void onSubscribe(Flow.Subscription subscription) {
            subscription.request(Long.MAX_VALUE);
        }

        @Override
        public void onNext(ByteBuffer item) {
            try {
                this.channel.write(item);
            } catch (IOException e) {
                this.completionFuture.completeExceptionally(e);
            }
        }

        @Override
        public void onError(Throwable throwable) {
            this.completionFuture.completeExceptionally(throwable);
        }

        @Override
        public void onComplete() {
            this.completionFuture.complete(null);
        }

        public byte[] getResult() throws Throwable {
            try {
                this.completionFuture.get();
                return outputStream.toByteArray();
            } catch (ExecutionException e) {
                throw e.getCause();
            }
        }
    }

    static class BufferedPublisher implements InvocationFlow.InvocationInputPublisher {

        private ByteBuffer buffer;

        BufferedPublisher(ByteBuffer buffer) {
            this.buffer = buffer.asReadOnlyBuffer();
        }

        @Override
        public void subscribe(Flow.Subscriber<? super ByteBuffer> subscriber) {
            subscriber.onSubscribe(
                    new Flow.Subscription() {
                        @Override
                        public void request(long l) {
                            if (buffer != null) {
                                subscriber.onNext(buffer);
                                subscriber.onComplete();
                                buffer = null;
                            }
                        }

                        @Override
                        public void cancel() {
                        }
                    });
        }
    }

}
