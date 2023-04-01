package cloud.phusion.express.controller;

import cloud.phusion.Context;
import cloud.phusion.DataObject;
import cloud.phusion.express.service.*;
import cloud.phusion.express.util.CommonCode;
import cloud.phusion.express.util.CommonHandler;
import cloud.phusion.express.util.ErrorResponse;
import cloud.phusion.protocol.http.HttpMethod;
import cloud.phusion.protocol.http.HttpRequest;
import cloud.phusion.protocol.http.HttpResponse;
import com.alibaba.fastjson2.JSONArray;
import com.alibaba.fastjson2.JSONObject;

import java.util.*;

public class ConnectionController {
    private static final String thisCategory = "connection";

    private static final Set<String> predefinedKeywords = new HashSet<>(
            Arrays.asList("operation")
    );

    public static void dispatch(JSONObject currentUser, HttpRequest req, HttpResponse resp, Context ctx) throws Exception {
        String item = req.getParameter("_item");
        String action = req.getMethod().toString();

        if (item == null) action += " null";
        else if (predefinedKeywords.contains(item)) action += " "+item;
        else action += " id";

        switch (action) {
            case "PUT null": _saveConnection(currentUser, req, resp, ctx); break;
            case "POST operation": _operateConnection(currentUser, req, resp, ctx); break;
            case "GET null":
                String countOnly = req.getParameter("countOnly");
                if ("true".equals(countOnly)) _countConnections(currentUser, req, resp, ctx);
                else _listConnections(currentUser, req, resp, ctx);
                break;
            case "GET id": _getConnection(currentUser, item, req, resp, ctx); break;
            case "DELETE id": _removeConnection(currentUser, item, req, resp, ctx); break;
            default: resp.setBody(ErrorResponse.compose("BAD_REQ_URL"));
        }
    }

    private static void _listConnections(JSONObject currentUser, HttpRequest req, HttpResponse resp, Context ctx) throws Exception {
        String ids = req.getParameter("ids");
        String fields = req.getParameter("fields");
        if (fields!=null && fields.length()==0) fields = null;

        DataObject result;

        if (ids==null || ids.length()==0) {
            String fromStr = req.getParameter("from");
            String lengthStr = req.getParameter("length");
            long from = (fromStr == null || fromStr.length() == 0) ? 0 : Long.parseLong(fromStr);
            long length = (lengthStr == null || lengthStr.length() == 0) ? 100 : Long.parseLong(lengthStr);

            String clientId = null;
            if (currentUser != null) {
                clientId = currentUser.getString("clientId");
                if (clientId!=null && clientId.length()==0) clientId = null;
            }

            Map<String,Object> params = new HashMap<>();
            params.put("clientId", clientId);
            params.put("fields", fields);
            if (! CommonHandler.checkAuthorizationResult(
                    AuthorizationService.checkPrivilege(currentUser, HttpMethod.GET, thisCategory, null, params, ctx),
                    resp)) return;

            result = ConnectionService.listConnections(null, clientId, fields, from, length, ctx);
        }
        else {
            result = ConnectionService.listConnectionsById(ids, fields, ctx);

            Map<String,Object> params = new HashMap<>();
            params.put("clientId", CommonCode.getCommonValueFromJSONArray(result,"clientId"));
            if (! CommonHandler.checkAuthorizationResult(
                    AuthorizationService.checkPrivilege(currentUser, HttpMethod.GET, thisCategory, null, params, ctx),
                    resp)) return;
        }

        resp.setBody(new DataObject("{\"result\":"+result.getString()+"}"));
    }

    private static void _countConnections(JSONObject currentUser, HttpRequest req, HttpResponse resp, Context ctx) throws Exception {
        String clientId = null;
        if (currentUser != null) {
            clientId = currentUser.getString("clientId");
            if (clientId!=null && clientId.length()==0) clientId = null;
        }

        Map<String,Object> params = new HashMap<>();
        params.put("clientId", clientId);
        if (! CommonHandler.checkAuthorizationResult(
                AuthorizationService.checkPrivilege(currentUser, HttpMethod.GET, thisCategory, null, params, ctx),
                resp)) return;

        long count = ConnectionService.countConnections(null, clientId, ctx);
        resp.setBody(new DataObject("{\"result\":"+count+"}"));
    }

    private static void _saveConnection(JSONObject currentUser, HttpRequest req, HttpResponse resp, Context ctx) throws Exception {
        JSONObject body = CommonHandler.parseDataToJSONObject(req.getBody(), resp, "BAD_REQ_BODY", ctx);
        if (body == null) return;

        if (! CommonHandler.checkExistence(body, new String[]{"id"}, resp)) return;
        if (! CommonHandler.checkImmutable(body, new String[]{"status","createTime","updateTime"}, resp)) return;

        String id = body.getString("id");
        if (! CommonHandler.checkIDPattern(id, "id", resp)) return;

        DataObject connDisk = ConnectionService.fetchConnection(id, ctx);
        JSONObject connObj = connDisk==null ? null : CommonCode.parseDataToJSONObject(connDisk, ctx);

        JSONObject connNew = CommonHandler.mergeJSONObjects(body, connObj,
                new String[]{"id","title","applicationId","clientId","desc"},
                new String[]{"config"},
                resp);
        if (connNew == null) return;

        Map<String,Object> params = new HashMap<>();
        params.put("clientId", connNew.getString("clientId"));
        if (! CommonHandler.checkAuthorizationResult(
                AuthorizationService.checkPrivilege(currentUser, HttpMethod.PUT, thisCategory, null, params, ctx),
                resp)) return;

        ConnectionService.saveConnection(id, new DataObject(connNew), ctx);
        resp.setBody(new DataObject("{}"));
    }

    private static void _getConnection(JSONObject currentUser, String id, HttpRequest req, HttpResponse resp, Context ctx) throws Exception {
        JSONObject connObj = _getConnectionFromDisk(id, resp, ctx);
        if (connObj == null) return;

        Map<String,Object> params = new HashMap<>();
        params.put("clientId", connObj.getString("clientId"));
        if (! CommonHandler.checkAuthorizationResult(
                AuthorizationService.checkPrivilege(currentUser, HttpMethod.GET, thisCategory, "id", params, ctx),
                resp)) return;

        JSONObject result = new JSONObject();

        String status = ConnectionService.getConnectionLocalStatus(id, connObj.getString("applicationId"), ctx);
        connObj.put("status", status);

        result.put("result", connObj);
        resp.setBody(new DataObject(result));
    }

    private static void _removeConnection(JSONObject currentUser, String id, HttpRequest req, HttpResponse resp, Context ctx) throws Exception {
        // Only check local status. To be optimized.
        if (! _checkConnection(id, "!"+ConnectionService.STATUS_RUN, "remove", "Connection is in use, or has integrations", resp, ctx)) return;

        Map<String,Object> params = new HashMap<>();
        params.put("id", id);
        if (! CommonHandler.checkAuthorizationResult(
                AuthorizationService.checkPrivilege(currentUser, HttpMethod.DELETE, thisCategory, "id", params, ctx),
                resp)) return;

        ConnectionService.removeConnection(id, ctx);
        resp.setBody(new DataObject("{}"));
    }

    private static void _operateConnection(JSONObject currentUser, HttpRequest req, HttpResponse resp, Context ctx) throws Exception {
        JSONObject body = CommonHandler.parseDataToJSONObject(req.getBody(), resp, "BAD_REQ_BODY", ctx);
        if (body == null) return;

        if (! CommonHandler.checkExistence(body, new String[]{"action","connectionId"}, resp)) return;

        String action = body.getString("action");
        String id = body.getString("connectionId");
        boolean local = body.getBooleanValue("local", false);

        Map<String,Object> params = new HashMap<>();
        params.put("id", id);
        if (! CommonHandler.checkAuthorizationResult(
                AuthorizationService.checkPrivilege(currentUser, HttpMethod.POST, thisCategory, "operation", params, ctx),
                resp)) return;

        if (local) {
            String status = null;
            String reason = null;

            switch (action) {
                case ConnectionService.ACTION_CONNECT:
                    status = "!" + ConnectionService.STATUS_RUN;
                    reason = "Connection is connected";
                    if (!_checkConnection(id, status, action, reason, resp, ctx)) return;
                    ConnectionService.startConnection(id, ctx);
                    break;
                case ConnectionService.ACTION_DISCONNECT:
                    status = ConnectionService.STATUS_RUN;
                    reason = "Connection is disconnected";
                    if (!_checkConnection(id, status, action, reason, resp, ctx)) return;
                    ConnectionService.stopConnection(id, ctx);
                    break;
                case ConnectionService.ACTION_RECONNECT:
                    if (!_checkConnection(id, status, action, reason, resp, ctx)) return;
                    ConnectionService.restartConnection(id, ctx);
                    break;
                default:
                    resp.setBody(ErrorResponse.compose("OP_NONE", "{\"action\":\"" + action + "\"}"));
                    return;
            }
        }
        else {
            if (!_checkConnection(id, null, action, null, resp, ctx)) return;

            if (action.equals(ConnectionService.ACTION_CONNECT) ||
                    action.equals(ConnectionService.ACTION_DISCONNECT) ||
                    action.equals(ConnectionService.ACTION_RECONNECT)) {
                ClusterService.sendMessage(ClusterService.OBJECT_CONNECTION, action, id, ctx);
            }
            else {
                resp.setBody(ErrorResponse.compose("OP_NONE", "{\"action\":\"" + action + "\"}"));
                return;
            }
        }

        resp.setBody(new DataObject("{}"));
    }

    //----------------------------

    private static JSONObject _getConnectionFromDisk(String id, HttpResponse resp, Context ctx) throws Exception {
        DataObject connDisk = ConnectionService.fetchConnection(id, ctx);

        if (connDisk == null) {
            resp.setBody(ErrorResponse.compose("NOT_FOUND", "{\"connectionId\":\""+id+"\"}"));
            return null;
        }

        return CommonHandler.parseDataToJSONObject(connDisk, resp, "BAD_ENTITY", "connectionId", id, ctx);
    }

    private static boolean _checkConnection(String id, String status, String action, String reason, HttpResponse resp, Context ctx) throws Exception {
        boolean result = false;

        if (ConnectionService.hasConnection(id, ctx)) {
            if (status != null) {
                DataObject connObj = ConnectionService.fetchConnection(id, ctx);
                String applicationid = connObj.getJSONObject().getString("applicationId");

                String statusConn = ConnectionService.getConnectionLocalStatus(id, applicationid, ctx);
                if (status.charAt(0) == '!') result = !statusConn.equals(status.substring(1));
                else result = statusConn.equals(status);
            }
            else result = true;

            if (action.equals("remove")) {
                // Check whether there are integrations
                Map<String,Object> query = new HashMap<>();
                query.put("connectionId", id);
                long count = IntegrationService.countIntegrations(query, ctx);
                if (count > 0) result = false;
            }

            if (! result) {
                String data = String.format("{\"connectionId\":\"%s\",\"action\":\"%s\",\"reason\":\"%s\"}", id, action, reason);
                resp.setBody(ErrorResponse.compose("OP_ERR", data));
            }
        }
        else
            resp.setBody(ErrorResponse.compose("NOT_FOUND", "{\"connectionId\":\""+id+"\"}"));

        return result;
    }

}
