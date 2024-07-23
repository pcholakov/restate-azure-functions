package dev.restate.examples.azure;

import com.microsoft.azure.functions.ExecutionContext;
import com.microsoft.azure.functions.HttpRequestMessage;
import com.microsoft.azure.functions.HttpResponseMessage;
import com.microsoft.azure.functions.HttpStatus;
import org.junit.jupiter.api.Test;
import org.mockito.stubbing.Answer;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.logging.Logger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;


public class RestateAzureFnAdapterTest {
//    @Test
//    public void testHttpTriggerJava() {
//        @SuppressWarnings("unchecked") final HttpRequestMessage<Optional<byte[]>> req = mock(HttpRequestMessage.class);
//
//        final Map<String, String> queryParams = new HashMap<>();
//        queryParams.put("name", "Azure");
//        doReturn(queryParams).when(req).getQueryParameters();
//
//        final Optional<String> queryBody = Optional.empty();
//        doReturn(queryBody).when(req).getBody();
//
//        doAnswer((Answer<HttpResponseMessage.Builder>) invocation -> {
//            HttpStatus status = (HttpStatus) invocation.getArguments()[0];
//            return new HttpResponseMessageMock.HttpResponseMessageBuilderMock().status(status);
//        }).when(req).createResponseBuilder(any(HttpStatus.class));
//
//        final ExecutionContext context = mock(ExecutionContext.class);
//        doReturn(Logger.getGlobal()).when(context).getLogger();
//
//        final HttpResponseMessage ret = new RestateAzureFnAdapter().run(req, context);
//
//        assertEquals(ret.getStatus(), HttpStatus.OK);
//    }
}
