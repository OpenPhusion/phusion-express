package cloud.phusion.express.controller;

import cloud.phusion.Context;
import cloud.phusion.DataObject;
import cloud.phusion.express.service.AuthorizationService;
import cloud.phusion.express.service.ClusterService;
import cloud.phusion.express.util.CommonHandler;
import cloud.phusion.express.util.ErrorResponse;
import cloud.phusion.protocol.http.HttpMethod;
import cloud.phusion.protocol.http.HttpRequest;
import cloud.phusion.protocol.http.HttpResponse;
import com.alibaba.fastjson2.JSONObject;

import java.util.HashMap;
import java.util.Map;

public class ClusterController {
    private static final String thisCategory = "cluster";

    public static void dispatch(JSONObject currentUser, HttpRequest req, HttpResponse resp, Context ctx) throws Exception {
        String item = req.getParameter("_item");
        String action = req.getMethod().toString();

        if (item == null) action += " null";
        else action += " "+item;

        switch (action) {
            case "GET engine": _listEngines(currentUser, req, resp, ctx); break;
            case "GET application": _listObjects(currentUser, ClusterService.OBJECT_APPLICATION, req, resp, ctx); break;
            case "GET connection": _listObjects(currentUser, ClusterService.OBJECT_CONNECTION, req, resp, ctx); break;
            case "GET integration": _listObjects(currentUser, ClusterService.OBJECT_INTEGRATION, req, resp, ctx); break;
            default: resp.setBody(ErrorResponse.compose("BAD_REQ_URL"));
        }
    }

    private static void _listEngines(JSONObject currentUser, HttpRequest req, HttpResponse resp, Context ctx) throws Exception {
        if (! CommonHandler.checkAuthorizationResult(
                AuthorizationService.checkPrivilege(currentUser, HttpMethod.GET, thisCategory, "engine", null, ctx),
                resp)) return;

        DataObject result = ClusterService.listEngines(ctx);
        resp.setBody(new DataObject("{\"result\":"+result.getString()+"}"));
    }

    private static void _listObjects(JSONObject currentUser, String type, HttpRequest req, HttpResponse resp, Context ctx) throws Exception {
        if (! CommonHandler.checkParamExistence(req, new String[]{"id"}, resp)) return;

        String id = req.getParameter("id");

        Map<String,Object> params = new HashMap<>();
        params.put("id", id);
        if (! CommonHandler.checkAuthorizationResult(
                AuthorizationService.checkPrivilege(currentUser, HttpMethod.GET, thisCategory, type, params, ctx),
                resp)) return;

        DataObject result = ClusterService.listObjectStatus(type, id, ctx);
        if (result == null) resp.setBody(new DataObject("{\"result\":[]}"));
        else resp.setBody(new DataObject("{\"result\":"+result.getString()+"}"));
    }

}
