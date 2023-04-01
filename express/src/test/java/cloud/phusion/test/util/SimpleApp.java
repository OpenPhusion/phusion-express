package cloud.phusion.test.util;

import cloud.phusion.*;
import cloud.phusion.application.*;
import cloud.phusion.protocol.http.HttpRequest;
import cloud.phusion.protocol.http.HttpResponse;

public class SimpleApp extends HttpBaseApplication {

    @OutboundEndpoint
    public DataObject queryOrders(DataObject msg, String integrationId,
                                            String connectionId, Context ctx) throws Exception {
        if (msg.getJSONObject().getString("name") == null) throw new Exception("Empty name!");
        return new DataObject("{\"status\":\"OK\"}");
    }

    @InboundEndpoint(address="/orders", connectionKeyInReqeust="conn")
    public void receiveChargeOrder(HttpRequest request,
                                           HttpResponse response,
                                           String[] integrationIds,
                                           String connectionId,
                                           Context ctx) throws Exception {

        DataObject result = ctx.getEngine().getIntegration(integrationIds[0]).execute(request.getBody(), ctx);
        response.setStatusCode(200);
        response.setBody(result);
    }

}
