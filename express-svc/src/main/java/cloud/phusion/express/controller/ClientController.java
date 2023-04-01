package cloud.phusion.express.controller;

import cloud.phusion.Context;
import cloud.phusion.DataObject;
import cloud.phusion.express.service.AuthorizationService;
import cloud.phusion.express.service.ClientService;
import cloud.phusion.express.service.ConnectionService;
import cloud.phusion.express.service.IntegrationService;
import cloud.phusion.express.util.CommonCode;
import cloud.phusion.express.util.CommonHandler;
import cloud.phusion.express.util.ErrorResponse;
import cloud.phusion.protocol.http.HttpMethod;
import cloud.phusion.protocol.http.HttpRequest;
import cloud.phusion.protocol.http.HttpResponse;
import com.alibaba.fastjson2.JSONArray;
import com.alibaba.fastjson2.JSONObject;

import java.util.*;

public class ClientController {
    private static final String thisCategory = "client";

    private static final Set<String> predefinedKeywords = new HashSet<>(
            Arrays.asList("table", "connection", "category")
    );

    public static void dispatch(JSONObject currentUser, HttpRequest req, HttpResponse resp, Context ctx) throws Exception {
        String item = req.getParameter("_item");
        String action = req.getMethod().toString();
        String countOnly;

        if (item == null) action += " null";
        else if (predefinedKeywords.contains(item)) action += " "+item;
        else action += " id";

        switch (action) {
            case "PUT null": _saveClient(currentUser, req, resp, ctx); break;
            case "PUT table": _saveDBTable(currentUser, req, resp, ctx); break;
            case "GET null":
                countOnly = req.getParameter("countOnly");
                if ("true".equals(countOnly)) _countClients(currentUser, req, resp, ctx);
                else _listClients(currentUser, req, resp, ctx);
                break;
            case "GET id": _getClient(currentUser, item, req, resp, ctx); break;
            case "GET table": _getDBTable(currentUser, req, resp, ctx); break;
            case "GET category": _listCategories(currentUser, req, resp, ctx); break;
            case "GET connection":
                countOnly = req.getParameter("countOnly");
                if (countOnly==null || countOnly.length()==0 || countOnly.equals("false"))
                    _listConnections(currentUser, req, resp, ctx);
                else
                    _countConnections(currentUser, req, resp, ctx);
                break;
            case "DELETE table": _removeDBTable(currentUser, req, resp, ctx); break;
            case "DELETE id": _removeClient(currentUser, item, req, resp, ctx); break;
            default: resp.setBody(ErrorResponse.compose("BAD_REQ_URL"));
        }
    }

    private static void _saveClient(JSONObject currentUser, HttpRequest req, HttpResponse resp, Context ctx) throws Exception {
        JSONObject body = CommonHandler.parseDataToJSONObject(req.getBody(), resp, "BAD_REQ_BODY", ctx);
        if (body == null) return;

        if (! CommonHandler.checkExistence(body, new String[]{"id"}, resp)) return;
        if (! CommonHandler.checkImmutable(body, new String[]{"tables","createTime","updateTime"}, resp)) return;

        String id = body.getString("id");
        if (! CommonHandler.checkIDPattern(id, "id", resp)) return;

        boolean categoryChanged = body.containsKey("categories");

        DataObject clientDisk = ClientService.fetchClient(id, ctx);
        JSONObject clientObj = clientDisk==null ? null : CommonCode.parseDataToJSONObject(clientDisk, ctx);

        JSONObject clientNew = CommonHandler.mergeJSONObjects(body, clientObj,
                new String[]{"id","title","icon","categories","desc"},
                null, resp);
        if (clientNew == null) return;

        Map<String,Object> params = new HashMap<>();
        params.put("id", id);
        if (! CommonHandler.checkAuthorizationResult(
                AuthorizationService.checkPrivilege(currentUser, HttpMethod.PUT, thisCategory, null, params, ctx),
                resp)) return;

        ClientService.saveClient(id, new DataObject(clientNew), categoryChanged, null, ctx);
        resp.setBody(new DataObject("{}"));
    }

    private static void _listConnections(JSONObject currentUser, HttpRequest req, HttpResponse resp, Context ctx) throws Exception {
        String id = req.getParameter("clientId");

        if (id==null && currentUser!=null) {
            id = currentUser.getString("clientId");
            if (id!=null && id.length()==0) id = null;
        }

        Map<String,Object> params = new HashMap<>();
        params.put("clientId", id);
        if (! CommonHandler.checkAuthorizationResult(
                AuthorizationService.checkPrivilege(currentUser, HttpMethod.GET, thisCategory, "connection", params, ctx),
                resp)) return;

        String fields = req.getParameter("fields");
        if (fields == null || fields.length() == 0) fields = "id,title,desc,status,applicationId";
        String fromStr = req.getParameter("from");
        String lengthStr = req.getParameter("length");
        long from = (fromStr == null || fromStr.length() == 0) ? 0 : Long.parseLong(fromStr);
        long length = (lengthStr == null || lengthStr.length() == 0) ? 100 : Long.parseLong(lengthStr);

        DataObject result = ConnectionService.listConnections(null, id, fields, from, length, ctx);

        resp.setBody(new DataObject("{\"result\":"+result.getString()+"}"));
    }

    private static void _countConnections(JSONObject currentUser, HttpRequest req, HttpResponse resp, Context ctx) throws Exception {
        String id = req.getParameter("clientId");

        if (id==null && currentUser!=null) {
            id = currentUser.getString("clientId");
            if (id!=null && id.length()==0) id = null;
        }

        Map<String,Object> params = new HashMap<>();
        params.put("clientId", id);
        if (! CommonHandler.checkAuthorizationResult(
                AuthorizationService.checkPrivilege(currentUser, HttpMethod.GET, thisCategory, "connection", params, ctx),
                resp)) return;

        long count = ConnectionService.countConnections(null, id, ctx);
        resp.setBody(new DataObject("{\"result\":"+count+"}"));
    }

    private static void _listCategories(JSONObject currentUser, HttpRequest req, HttpResponse resp, Context ctx) throws Exception {
        if (! CommonHandler.checkAuthorizationResult(
                AuthorizationService.checkPrivilege(currentUser, HttpMethod.GET, thisCategory, "category", null, ctx),
                resp)) return;

        DataObject result = ClientService.listCategories(ctx);
        resp.setBody(new DataObject("{\"result\":"+result.getString()+"}"));
    }

    private static void _listClients(JSONObject currentUser, HttpRequest req, HttpResponse resp, Context ctx) throws Exception {
        String ids = req.getParameter("ids");
        String fields = req.getParameter("fields");
        if (ids!=null && ids.length()==0) ids = null;
        if (fields!=null && fields.length()==0) fields = null;

        Map<String,Object> params = new HashMap<>();
        params.put("clientIds", CommonCode.convertStringToSet(ids));
        if (! CommonHandler.checkAuthorizationResult(
                AuthorizationService.checkPrivilege(currentUser, HttpMethod.GET, thisCategory, null, params, ctx),
                resp)) return;

        DataObject result;

        if (ids == null) {
            String search = req.getParameter("search");
            String category = req.getParameter("category");
            if (search!=null && search.length()==0) search = null;
            if (category!=null && category.length()==0) category = null;
            String fromStr = req.getParameter("from");
            String lengthStr = req.getParameter("length");
            long from = (fromStr == null || fromStr.length() == 0) ? 0 : Long.parseLong(fromStr);
            long length = (lengthStr == null || lengthStr.length() == 0) ? 100 : Long.parseLong(lengthStr);

            Map<String,Object> query = new HashMap<>();
            query.put("search", search);
            query.put("category", category);
            result = ClientService.queryClients(query, fields, from, length, ctx);
        }
        else {
            result = ClientService.listClientsById(ids, fields, ctx);
        }

        resp.setBody(new DataObject("{\"result\":"+result.getString()+"}"));
    }

    private static void _countClients(JSONObject currentUser, HttpRequest req, HttpResponse resp, Context ctx) throws Exception {
        if (! CommonHandler.checkAuthorizationResult(
                AuthorizationService.checkPrivilege(currentUser, HttpMethod.GET, thisCategory, null, null, ctx),
                resp)) return;

        String search = req.getParameter("search");
        String category = req.getParameter("category");
        if (search!=null && search.length()==0) search = null;
        if (category!=null && category.length()==0) category = null;

        Map<String,Object> query = new HashMap<>();
        query.put("search", search);
        query.put("category", category);
        long count = ClientService.countClients(query, ctx);
        resp.setBody(new DataObject("{\"result\":"+count+"}"));
    }

    private static void _getClient(JSONObject currentUser, String id, HttpRequest req, HttpResponse resp, Context ctx) throws Exception {
        Map<String,Object> params = new HashMap<>();
        params.put("id", id);
        if (! CommonHandler.checkAuthorizationResult(
                AuthorizationService.checkPrivilege(currentUser, HttpMethod.GET, thisCategory, "id", params, ctx),
                resp)) return;

        JSONObject clientObj = _getClientFromDisk(id, resp, ctx);
        if (clientObj == null) return;

        JSONObject result = new JSONObject();
        result.put("result", clientObj);
        resp.setBody(new DataObject(result));
    }

    private static void _saveDBTable(JSONObject currentUser, HttpRequest req, HttpResponse resp, Context ctx) throws Exception {
        JSONObject body = CommonHandler.parseDataToJSONObject(req.getBody(), resp, "BAD_REQ_BODY", ctx);
        if (body == null) return;

        if (! CommonHandler.checkExistence(body, new String[]{"name","clientId"}, resp)) return;

        String tableId = body.getString("name");
        if (! CommonHandler.checkIDPattern(tableId, "name", resp)) return;

        String clientId = body.getString("clientId");
        body.remove("clientId");

        Map<String,Object> params = new HashMap<>();
        params.put("clientId", clientId);
        if (! CommonHandler.checkAuthorizationResult(
                AuthorizationService.checkPrivilege(currentUser, HttpMethod.PUT, thisCategory, "table", params, ctx),
                resp)) return;

        JSONObject clientObj = _getClientFromDisk(clientId, resp, ctx);
        if (clientObj == null) return;

        JSONObject tableObj = CommonCode.findItemInJSONArray(clientObj.getJSONArray("tables"), "name", tableId);
        JSONObject tableNew = CommonHandler.mergeJSONObjects(body, tableObj,
                new String[]{"name","fields","desc","indexes"},
                null,
                resp);
        if (tableNew == null) return;

        CommonCode.insertItemIntoJSONArray(clientObj, "tables", "name", tableNew);

        ClientService.saveClient(clientId, new DataObject(clientObj), false, tableId, ctx);
        resp.setBody(new DataObject("{}"));
    }

    private static void _getDBTable(JSONObject currentUser, HttpRequest req, HttpResponse resp, Context ctx) throws Exception {
        JSONObject body = CommonHandler.getParamsFromBodyOrParams(req, new String[]{"name","clientId"}, resp, ctx);
        if (body == null) return;

        String tableId = body.getString("name");
        String clientId = body.getString("clientId");

        Map<String,Object> params = new HashMap<>();
        params.put("clientId", clientId);
        if (! CommonHandler.checkAuthorizationResult(
                AuthorizationService.checkPrivilege(currentUser, HttpMethod.GET, thisCategory, "table", params, ctx),
                resp)) return;

        JSONObject clientObj = _getClientFromDisk(clientId, resp, ctx);
        if (clientObj == null) return;

        JSONArray arr = clientObj.getJSONArray("tables");
        String data = "{\"clientId\":\""+clientId+"\",\"tableName\":\""+tableId+"\"}";
        int pos = CommonHandler.findItemInJSONArray(arr, "name", tableId, data, resp);
        if (pos < 0) return;

        JSONObject result = new JSONObject();
        result.put("result", arr.getJSONObject(pos));
        resp.setBody(new DataObject(result));
    }

    private static void _removeDBTable(JSONObject currentUser, HttpRequest req, HttpResponse resp, Context ctx) throws Exception {
        if (! CommonHandler.checkParamExistence(req, new String[]{"name","clientId"}, resp)) return;

        String tableId = req.getParameter("name");
        String clientId = req.getParameter("clientId");

        Map<String,Object> params = new HashMap<>();
        params.put("clientId", clientId);
        if (! CommonHandler.checkAuthorizationResult(
                AuthorizationService.checkPrivilege(currentUser, HttpMethod.DELETE, thisCategory, "table", params, ctx),
                resp)) return;

        JSONObject clientObj = _getClientFromDisk(clientId, resp, ctx);
        if (clientObj == null) return;

        JSONArray arr = clientObj.getJSONArray("tables");
        String data = "{\"clientId\":\""+clientId+"\",\"tableName\":\""+tableId+"\"}";
        int pos = CommonHandler.findItemInJSONArray(arr, "name", tableId, data, resp);
        if (pos < 0) return;

        arr.remove(pos);
        ClientService.saveClient(clientId, new DataObject(clientObj), false, null, ctx);

        // Remove table physically

        boolean physical = "true".equals(req.getParameter("removePhysically"));
        if (physical) CommonCode.removeTablePhysically("client", clientId, tableId, ctx);

        resp.setBody(new DataObject("{}"));
    }

    private static void _removeClient(JSONObject currentUser, String id, HttpRequest req, HttpResponse resp, Context ctx) throws Exception {
        Map<String,Object> params = new HashMap<>();
        params.put("id", id);
        if (! CommonHandler.checkAuthorizationResult(
                AuthorizationService.checkPrivilege(currentUser, HttpMethod.DELETE, thisCategory, "id", params, ctx),
                resp)) return;

        if (! _checkClient(id, "remove", "Client has DB tables or integrations", resp, ctx)) return;

        ClientService.removeClient(id, ctx);
        resp.setBody(new DataObject("{}"));
    }

    //----------------------------

    private static JSONObject _getClientFromDisk(String id, HttpResponse resp, Context ctx) throws Exception {
        DataObject clientDisk = ClientService.fetchClient(id, ctx);

        if (clientDisk == null) {
            resp.setBody(ErrorResponse.compose("NOT_FOUND", "{\"clientId\":\""+id+"\"}"));
            return null;
        }

        return CommonHandler.parseDataToJSONObject(clientDisk, resp, "BAD_ENTITY", "clientId", id, ctx);
    }

    private static boolean _checkClient(String id, String action, String reason, HttpResponse resp, Context ctx) throws Exception {
        boolean result = false;

        if (ClientService.hasClient(id, ctx)) {
            result = true;

            if (action.equals("remove")) {
                // Check whether there are DB tables
                DataObject clientDisk = ClientService.fetchClient(id, ctx);
                JSONObject clientObj = clientDisk.getJSONObject();
                JSONArray tables = clientObj.getJSONArray("tables");
                if (tables!=null && tables.size()>0) result = false;

                // Check whether there are integrations
                Map<String,Object> query = new HashMap<>();
                query.put("clientId", id);
                long count = IntegrationService.countIntegrations(query, ctx);
                if (count > 0) result = false;
            }

            if (! result) {
                String data = String.format("{\"clientId\":\"%s\",\"action\":\"%s\",\"reason\":\"%s\"}", id, action, reason);
                resp.setBody(ErrorResponse.compose("OP_ERR", data));
            }
        }
        else
            resp.setBody(ErrorResponse.compose("NOT_FOUND", "{\"clientId\":\""+id+"\"}"));

        return result;
    }

}
