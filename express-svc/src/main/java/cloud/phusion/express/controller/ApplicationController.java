package cloud.phusion.express.controller;

import cloud.phusion.Context;
import cloud.phusion.DataObject;
import cloud.phusion.PhusionException;
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

/**
 * The schemas are stored separately from applications.
 */
public class ApplicationController {
    private static final String thisCategory = "application";

    private static final Set<String> predefinedKeywords = new HashSet<>(
            Arrays.asList("table", "endpoint", "operation", "connection", "category", "protocol")
    );

    public static void dispatch(JSONObject currentUser, HttpRequest req, HttpResponse resp, Context ctx) throws Exception {
        String item = req.getParameter("_item");
        String action = req.getMethod().toString();
        String countOnly;

        if (item == null) action += " null";
        else if (predefinedKeywords.contains(item)) action += " "+item;
        else action += " id";

        switch (action) {
            case "PUT null": _saveApplication(currentUser, req, resp, ctx); break;
            case "PUT endpoint": _saveEndpoint(currentUser, req, resp, ctx); break;
            case "PUT table": _saveDBTable(currentUser, req, resp, ctx); break;
            case "POST operation": _operateApplication(currentUser, req, resp, ctx); break;
            case "GET null":
                countOnly = req.getParameter("countOnly");
                if ("true".equals(countOnly)) _countApplications(currentUser, req, resp, ctx);
                else _listApplications(currentUser, req, resp, ctx);
                break;
            case "GET id": _getApplication(currentUser, item, req, resp, ctx); break;
            case "GET endpoint": _getEndpoint(currentUser, req, resp, ctx); break;
            case "GET table": _getDBTable(currentUser, req, resp, ctx); break;
            case "GET category": _listCategories(currentUser, req, resp, ctx); break;
            case "GET protocol": _listProtocols(currentUser, req, resp, ctx); break;
            case "GET connection":
                countOnly = req.getParameter("countOnly");
                if (countOnly==null || countOnly.length()==0 || countOnly.equals("false"))
                    _listConnections(currentUser, req, resp, ctx);
                else
                    _countConnections(currentUser, req, resp, ctx);
                break;
            case "DELETE endpoint": _removeEndpoint(currentUser, req, resp, ctx); break;
            case "DELETE table": _removeDBTable(currentUser, req, resp, ctx); break;
            case "DELETE id": _removeApplication(currentUser, item, req, resp, ctx); break;
            default: resp.setBody(ErrorResponse.compose("BAD_REQ_URL"));
        }
    }

    private static void _saveApplication(JSONObject currentUser, HttpRequest req, HttpResponse resp, Context ctx) throws Exception {
        JSONObject body = CommonHandler.parseDataToJSONObject(req.getBody(), resp, "BAD_REQ_BODY", ctx);
        if (body == null) return;

        if (! CommonHandler.checkExistence(body, new String[]{"id"}, resp)) return;
        if (! CommonHandler.checkImmutable(body, new String[]{"status","endpoints","tables","createTime","updateTime"}, resp)) return;

        JSONObject code = body.getJSONObject("code");
        if (code!=null && code.size()>0) {
            if (! CommonHandler.checkModulePattern(code.getString("module"), "code.module", resp)) return;
        }

        String id = body.getString("id");
        if (! CommonHandler.checkIDPattern(id, "id", resp)) return;

        Map<String,Object> params = new HashMap<>();
        params.put("id", id);
        if (! CommonHandler.checkAuthorizationResult(
                AuthorizationService.checkPrivilege(currentUser, HttpMethod.PUT, thisCategory, null, params, ctx),
                resp)) return;

        boolean categoryChanged = body.containsKey("categories");
        boolean protocolChanged = body.containsKey("protocols");

        DataObject appDisk = ApplicationService.fetchApplication(id, ctx);
        JSONObject appObj = appDisk==null ? null : CommonCode.parseDataToJSONObject(appDisk, ctx);

        ApplicationService.saveApplicationSchemasFromObject(id, body, ctx);

        JSONObject appNew = CommonHandler.mergeJSONObjects(body, appObj,
                new String[]{"id","autoStart","abstract","title","doc","icon","categories","protocols","desc"},
                new String[]{"config","code"},
                resp);
        if (appNew == null) return;

        if (appNew.getBooleanValue("abstract", false)) {
            appNew.put("autoStart", false);
        }

        ApplicationService.saveApplication(id, new DataObject(appNew), categoryChanged, protocolChanged, null, ctx);
        resp.setBody(new DataObject("{}"));
    }

    private static void _listCategories(JSONObject currentUser, HttpRequest req, HttpResponse resp, Context ctx) throws Exception {
        if (! CommonHandler.checkAuthorizationResult(
                AuthorizationService.checkPrivilege(currentUser, HttpMethod.GET, thisCategory, "category", null, ctx),
                resp)) return;

        DataObject result = ApplicationService.listCategories(ctx);
        resp.setBody(new DataObject("{\"result\":"+result.getString()+"}"));
    }

    private static void _listProtocols(JSONObject currentUser, HttpRequest req, HttpResponse resp, Context ctx) throws Exception {
        if (! CommonHandler.checkAuthorizationResult(
                AuthorizationService.checkPrivilege(currentUser, HttpMethod.GET, thisCategory, "protocol", null, ctx),
                resp)) return;

        DataObject result = ApplicationService.listProtocols(ctx);
        resp.setBody(new DataObject("{\"result\":"+result.getString()+"}"));
    }

    private static void _listConnections(JSONObject currentUser, HttpRequest req, HttpResponse resp, Context ctx) throws Exception {
        if (! CommonHandler.checkParamExistence(req, new String[]{"applicationId"}, resp)) return;

        String id = req.getParameter("applicationId");
        String fields = req.getParameter("fields");
        if (fields == null || fields.length() == 0) fields = "id,title,desc,status,clientId";
        String fromStr = req.getParameter("from");
        String lengthStr = req.getParameter("length");
        long from = (fromStr == null || fromStr.length() == 0) ? 0 : Long.parseLong(fromStr);
        long length = (lengthStr == null || lengthStr.length() == 0) ? 100 : Long.parseLong(lengthStr);

        Map<String,Object> params = new HashMap<>();
        params.put("applicationId", id);
        if (! CommonHandler.checkAuthorizationResult(
                AuthorizationService.checkPrivilege(currentUser, HttpMethod.GET, thisCategory, "connection", params, ctx),
                resp)) return;

        DataObject result = ConnectionService.listConnections(id, null, fields, from, length, ctx);

        resp.setBody(new DataObject("{\"result\":"+result.getString()+"}"));
    }

    private static void _countConnections(JSONObject currentUser, HttpRequest req, HttpResponse resp, Context ctx) throws Exception {
        if (! CommonHandler.checkParamExistence(req, new String[]{"applicationId"}, resp)) return;

        String id = req.getParameter("applicationId");

        Map<String,Object> params = new HashMap<>();
        params.put("applicationId", id);
        if (! CommonHandler.checkAuthorizationResult(
                AuthorizationService.checkPrivilege(currentUser, HttpMethod.GET, thisCategory, "connection", params, ctx),
                resp)) return;

        long count = ConnectionService.countConnections(id, null, ctx);
        resp.setBody(new DataObject("{\"result\":"+count+"}"));
    }

    private static void _listApplications(JSONObject currentUser, HttpRequest req, HttpResponse resp, Context ctx) throws Exception {
        String ids = req.getParameter("ids");
        String fields = req.getParameter("fields");
        if (fields!=null && fields.length()==0) fields = null;

        if (! CommonHandler.checkAuthorizationResult(
                AuthorizationService.checkPrivilege(currentUser, HttpMethod.GET, thisCategory, null, null, ctx),
                resp)) return;

        DataObject result;

        if (ids==null || ids.length()==0) {
            String search = req.getParameter("search");
            String category = req.getParameter("category");
            String protocol = req.getParameter("protocol");
            String abstractStr = req.getParameter("abstract");
            if (search!=null && search.length()==0) search = null;
            if (category!=null && category.length()==0) category = null;
            if (protocol!=null && protocol.length()==0) protocol = null;
            if (abstractStr!=null && abstractStr.length()==0) abstractStr = null;

            String fromStr = req.getParameter("from");
            String lengthStr = req.getParameter("length");
            long from = (fromStr == null || fromStr.length() == 0) ? 0 : Long.parseLong(fromStr);
            long length = (lengthStr == null || lengthStr.length() == 0) ? 100 : Long.parseLong(lengthStr);

            Map<String,Object> query = new HashMap<>();
            query.put("search", search);
            query.put("category", category);
            query.put("protocol", protocol);
            query.put("abstractStr", abstractStr);

            result = ApplicationService.queryApplications(query, fields, from, length, ctx);
        }
        else {
            result = ApplicationService.listApplicationsById(ids, fields, ctx);
        }

        resp.setBody(new DataObject("{\"result\":"+result.getString()+"}"));
    }

    private static void _countApplications(JSONObject currentUser, HttpRequest req, HttpResponse resp, Context ctx) throws Exception {
        String search = req.getParameter("search");
        String category = req.getParameter("category");
        String protocol = req.getParameter("protocol");
        String abstractStr = req.getParameter("abstract");
        if (search!=null && search.length()==0) search = null;
        if (category!=null && category.length()==0) category = null;
        if (protocol!=null && protocol.length()==0) protocol = null;
        if (abstractStr!=null && abstractStr.length()==0) abstractStr = null;

        Map<String,Object> query = new HashMap<>();
        query.put("search", search);
        query.put("category", category);
        query.put("protocol", protocol);
        query.put("abstractStr", abstractStr);

        if (! CommonHandler.checkAuthorizationResult(
                AuthorizationService.checkPrivilege(currentUser, HttpMethod.GET, thisCategory, null, null, ctx),
                resp)) return;

        long count = ApplicationService.countApplications(query, ctx);
        resp.setBody(new DataObject("{\"result\":"+count+"}"));
    }

    private static void _getApplication(JSONObject currentUser, String id, HttpRequest req, HttpResponse resp, Context ctx) throws Exception {
        JSONObject appObj = _getApplicationFromDisk(id, resp, ctx);
        if (appObj == null) return;

        Map<String,Object> params = new HashMap<>();
        params.put("id", id);
        if (! CommonHandler.checkAuthorizationResult(
                AuthorizationService.checkPrivilege(currentUser, HttpMethod.GET, thisCategory, "id", params, ctx),
                resp)) return;

        String hideSchemaStr = req.getParameter("hideSchemas");
        boolean hideSchema = "true".equals(hideSchemaStr);
        if (! hideSchema) ApplicationService.fetchSchemas(appObj, ctx);

        String status = ApplicationService.getApplicationLocalStatus(id, ctx);
        appObj.put("status", status);

        JSONObject result = new JSONObject();
        result.put("result", appObj);
        resp.setBody(new DataObject(result));
    }

    private static void _saveEndpoint(JSONObject currentUser, HttpRequest req, HttpResponse resp, Context ctx) throws Exception {
        JSONObject body = CommonHandler.parseDataToJSONObject(req.getBody(), resp, "BAD_REQ_BODY", ctx);
        if (body == null) return;

        if (! CommonHandler.checkExistence(body, new String[]{"id","applicationId"}, resp)) return;

        String endpointId = body.getString("id");
        if (! CommonHandler.checkIDPattern(endpointId, "id", resp)) return;

        String applicationId = body.getString("applicationId");
        body.remove("applicationId");

        Map<String,Object> params = new HashMap<>();
        params.put("applicationId", applicationId);
        if (! CommonHandler.checkAuthorizationResult(
                AuthorizationService.checkPrivilege(currentUser, HttpMethod.PUT, thisCategory, "endpoint", params, ctx),
                resp)) return;

        JSONObject appObj = _getApplicationFromDisk(applicationId, resp, ctx);
        if (appObj == null) return;

        ApplicationService.saveEndpointSchemasFromObject(applicationId, endpointId, body, ctx);

        JSONObject endpointObj = CommonCode.findItemInJSONArray(appObj.getJSONArray("endpoints"), "id", endpointId);
        JSONObject endpointNew = CommonHandler.mergeJSONObjects(body, endpointObj,
                new String[]{"id","title","desc","direction","address","httpMethod"},
                null,
                resp);
        if (endpointNew == null) return;

        CommonCode.insertItemIntoJSONArray(appObj, "endpoints", "id", endpointNew);

        ApplicationService.saveApplication(applicationId, new DataObject(appObj), false, false, null, ctx);
        resp.setBody(new DataObject("{}"));
    }

    private static void _getEndpoint(JSONObject currentUser, HttpRequest req, HttpResponse resp, Context ctx) throws Exception {
        JSONObject body = CommonHandler.getParamsFromBodyOrParams(req, new String[]{"id","applicationId"}, resp, ctx);
        if (body == null) return;

        String endpointId = body.getString("id");
        String applicationId = body.getString("applicationId");

        Map<String,Object> params = new HashMap<>();
        params.put("applicationId", applicationId);
        if (! CommonHandler.checkAuthorizationResult(
                AuthorizationService.checkPrivilege(currentUser, HttpMethod.GET, thisCategory, "endpoint", params, ctx),
                resp)) return;

        JSONObject appObj = _getApplicationFromDisk(applicationId, resp, ctx);
        if (appObj == null) return;

        JSONArray arr = appObj.getJSONArray("endpoints");
        String data = "{\"applicationId\":\""+applicationId+"\",\"endpointId\":\""+endpointId+"\"}";
        int pos = CommonHandler.findItemInJSONArray(arr, "id", endpointId, data, resp);
        if (pos < 0) return;

        JSONObject endpointObj = arr.getJSONObject(pos);
        ApplicationService.fetchEndpointSchemasIntoObject(applicationId, endpointId, endpointObj, ctx);

        JSONObject result = new JSONObject();
        result.put("result", endpointObj);
        resp.setBody(new DataObject(result));
    }

    private static void _removeEndpoint(JSONObject currentUser, HttpRequest req, HttpResponse resp, Context ctx) throws Exception {
        if (! CommonHandler.checkParamExistence(req, new String[]{"id","applicationId"}, resp)) return;

        String endpointId = req.getParameter("id");
        String applicationId = req.getParameter("applicationId");

        Map<String,Object> params = new HashMap<>();
        params.put("applicationId", applicationId);
        if (! CommonHandler.checkAuthorizationResult(
                AuthorizationService.checkPrivilege(currentUser, HttpMethod.DELETE, thisCategory, "endpoint", params, ctx),
                resp)) return;

        JSONObject appObj = _getApplicationFromDisk(applicationId, resp, ctx);
        if (appObj == null) return;

        JSONArray arr = appObj.getJSONArray("endpoints");
        String data = "{\"applicationId\":\""+applicationId+"\",\"endpointId\":\""+endpointId+"\"}";
        int pos = CommonHandler.findItemInJSONArray(arr, "id", endpointId, data, resp);
        if (pos < 0) return;

        arr.remove(pos);
        ApplicationService.saveApplication(applicationId, new DataObject(appObj), false, false, null, ctx);
        ApplicationService.removeEndpointSchemas(applicationId, endpointId, ctx);

        resp.setBody(new DataObject("{}"));
    }

    private static void _saveDBTable(JSONObject currentUser, HttpRequest req, HttpResponse resp, Context ctx) throws Exception {
        JSONObject body = CommonHandler.parseDataToJSONObject(req.getBody(), resp, "BAD_REQ_BODY", ctx);
        if (body == null) return;

        if (! CommonHandler.checkExistence(body, new String[]{"name","applicationId"}, resp)) return;

        String tableId = body.getString("name");
        if (! CommonHandler.checkIDPattern(tableId, "name", resp)) return;

        String applicationId = body.getString("applicationId");
        body.remove("applicationId");

        Map<String,Object> params = new HashMap<>();
        params.put("applicationId", applicationId);
        if (! CommonHandler.checkAuthorizationResult(
                AuthorizationService.checkPrivilege(currentUser, HttpMethod.PUT, thisCategory, "table", params, ctx),
                resp)) return;

        JSONObject appObj = _getApplicationFromDisk(applicationId, resp, ctx);
        if (appObj == null) return;

        JSONObject tableObj = CommonCode.findItemInJSONArray(appObj.getJSONArray("tables"), "name", tableId);
        JSONObject tableNew = CommonHandler.mergeJSONObjects(body, tableObj,
                new String[]{"name","fields","desc","indexes"},
                null,
                resp);
        if (tableNew == null) return;

        CommonCode.insertItemIntoJSONArray(appObj, "tables", "name", tableNew);

        ApplicationService.saveApplication(applicationId, new DataObject(appObj), false, false, tableId, ctx);
        resp.setBody(new DataObject("{}"));
    }

    private static void _getDBTable(JSONObject currentUser, HttpRequest req, HttpResponse resp, Context ctx) throws Exception {
        JSONObject body = CommonHandler.getParamsFromBodyOrParams(req, new String[]{"name","applicationId"}, resp, ctx);
        if (body == null) return;

        String tableId = body.getString("name");
        String applicationId = body.getString("applicationId");

        Map<String,Object> params = new HashMap<>();
        params.put("applicationId", applicationId);
        if (! CommonHandler.checkAuthorizationResult(
                AuthorizationService.checkPrivilege(currentUser, HttpMethod.GET, thisCategory, "table", params, ctx),
                resp)) return;

        JSONObject appObj = _getApplicationFromDisk(applicationId, resp, ctx);
        if (appObj == null) return;

        JSONArray arr = appObj.getJSONArray("tables");
        String data = "{\"applicationId\":\""+applicationId+"\",\"tableName\":\""+tableId+"\"}";
        int pos = CommonHandler.findItemInJSONArray(arr, "name", tableId, data, resp);
        if (pos < 0) return;

        JSONObject result = new JSONObject();
        result.put("result", arr.getJSONObject(pos));
        resp.setBody(new DataObject(result));
    }

    private static void _removeDBTable(JSONObject currentUser, HttpRequest req, HttpResponse resp, Context ctx) throws Exception {
        if (! CommonHandler.checkParamExistence(req, new String[]{"name","applicationId"}, resp)) return;

        String tableId = req.getParameter("name");
        String applicationId = req.getParameter("applicationId");

        Map<String,Object> params = new HashMap<>();
        params.put("applicationId", applicationId);
        if (! CommonHandler.checkAuthorizationResult(
                AuthorizationService.checkPrivilege(currentUser, HttpMethod.DELETE, thisCategory, "table", params, ctx),
                resp)) return;

        JSONObject appObj = _getApplicationFromDisk(applicationId, resp, ctx);
        if (appObj == null) return;

        JSONArray arr = appObj.getJSONArray("tables");
        String data = "{\"applicationId\":\""+applicationId+"\",\"tableName\":\""+tableId+"\"}";
        int pos = CommonHandler.findItemInJSONArray(arr, "name", tableId, data, resp);
        if (pos < 0) return;

        arr.remove(pos);
        ApplicationService.saveApplication(applicationId, new DataObject(appObj), false, false, null, ctx);

        // Remove table physically

        boolean physical = "true".equals(req.getParameter("removePhysically"));
        if (physical) CommonCode.removeTablePhysically("application", applicationId, tableId, ctx);

        resp.setBody(new DataObject("{}"));
    }

    private static void _removeApplication(JSONObject currentUser, String id, HttpRequest req, HttpResponse resp, Context ctx) throws Exception {
        Map<String,Object> params = new HashMap<>();
        params.put("id", id);
        if (! CommonHandler.checkAuthorizationResult(
                AuthorizationService.checkPrivilege(currentUser, HttpMethod.DELETE, thisCategory, "id", params, ctx),
                resp)) return;

        // Only check local status. To be optimized.
        if (! _checkApplication(id, "!"+ApplicationService.STATUS_RUN, "remove", "Application is running, or has DB tables or integrations or associated applications (through protocols)", resp, ctx)) return;

        ApplicationService.removeApplication(id, ctx);
        resp.setBody(new DataObject("{}"));
    }

    private static void _operateApplication(JSONObject currentUser, HttpRequest req, HttpResponse resp, Context ctx) throws Exception {
        JSONObject body = CommonHandler.parseDataToJSONObject(req.getBody(), resp, "BAD_REQ_BODY", ctx);
        if (body == null) return;

        if (! CommonHandler.checkExistence(body, new String[]{"action","applicationId"}, resp)) return;

        String action = body.getString("action");
        String id = body.getString("applicationId");
        boolean local = body.getBooleanValue("local", false);

        Map<String,Object> params = new HashMap<>();
        params.put("id", id);
        if (! CommonHandler.checkAuthorizationResult(
                AuthorizationService.checkPrivilege(currentUser, HttpMethod.POST, thisCategory, "operation", params, ctx),
                resp)) return;

        String status = null;
        String reason = null;

        if (local) {
            try {
                switch (action) {
                    case ApplicationService.ACTION_START:
                        status = "!" + ApplicationService.STATUS_RUN;
                        reason = "Application is running";
                        if (!_checkApplication(id, status, action, reason, resp, ctx)) return;
                        ApplicationService.startApplication(id, ctx);
                        break;
                    case ApplicationService.ACTION_STOP:
                        status = ApplicationService.STATUS_RUN;
                        reason = "Application is not running";
                        if (!_checkApplication(id, status, action, reason, resp, ctx)) return;
                        ApplicationService.stopApplication(id, ctx);
                        break;
                    case ApplicationService.ACTION_RESTART:
                        if (!_checkApplication(id, status, action, reason, resp, ctx)) return;
                        ApplicationService.restartApplication(id, ctx);
                        break;
                    default:
                        resp.setBody(ErrorResponse.compose("OP_NONE", "{\"action\":\"" + action + "\"}"));
                        return;
                }
            } catch (PhusionException ex) {
                String code = ex.getCode();
                reason = null;

                switch (code) {
                    case "APP_REL_IT":
                        if (reason == null) reason = "There are associated integrations running";
                    case "APP_DEF_ERR":
                        if (reason == null) reason = "Code not defined or uploaded";

                        String data = String.format("{\"applicationId\":\"%s\",\"action\":\"%s\",\"reason\":\"%s\"}", id, action, reason);
                        resp.setBody(ErrorResponse.compose("OP_ERR", data));
                        return;
                    default:
                        throw ex;
                }
            }
        }
        else {
            reason = "There are associated integrations running";
            if (!_checkApplication(id, status, action, reason, resp, ctx)) return;

            if (action.equals(ApplicationService.ACTION_START) ||
                    action.equals(ApplicationService.ACTION_STOP) ||
                    action.equals(ApplicationService.ACTION_RESTART)) {
                ClusterService.sendMessage(ClusterService.OBJECT_APPLICATION, action, id, ctx);
            }
            else {
                resp.setBody(ErrorResponse.compose("OP_NONE", "{\"action\":\"" + action + "\"}"));
                return;
            }
        }

        resp.setBody(new DataObject("{}"));
    }

    //----------------------------

    private static JSONObject _getApplicationFromDisk(String id, HttpResponse resp, Context ctx) throws Exception {
        DataObject appDisk = ApplicationService.fetchApplication(id, ctx);

        if (appDisk == null) {
            resp.setBody(ErrorResponse.compose("NOT_FOUND", "{\"applicationId\":\""+id+"\"}"));
            return null;
        }

        return CommonHandler.parseDataToJSONObject(appDisk, resp, "BAD_ENTITY", "applictionId", id, ctx);
    }

    private static boolean _checkApplication(String id, String status, String action, String reason, HttpResponse resp, Context ctx) throws Exception {
        boolean result = false;

        if (ApplicationService.hasApplication(id, ctx)) {
            if (status != null) {
                String statusApp = ApplicationService.getApplicationLocalStatus(id, ctx);
                if (status.charAt(0) == '!') result = !statusApp.equals(status.substring(1));
                else result = statusApp.equals(status);
            }
            else result = true;

            if (action.equals("remove")) {
                // Check whether there are DB tables
                DataObject appDisk = ApplicationService.fetchApplication(id, ctx);
                JSONObject appObj = appDisk.getJSONObject();
                JSONArray tables = appObj.getJSONArray("tables");
                if (tables!=null && tables.size()>0) result = false;

                // Check whether there are integrations
                Map<String,Object> query = new HashMap<>();
                query.put("applicationId", id);
                long count = IntegrationService.countIntegrations(query, ctx);
                if (count > 0) result = false;

                // Check whether there are applications associated with protocols
                query = new HashMap<>();
                query.put("protocol", id);
                count = ApplicationService.countApplications(query, ctx);
                if (count > 0) result = false;
            }
            else if (action.equals("stop") || action.equals("restart")) {
                Map<String,Object> query = new HashMap<>();
                query.put("applicationId", id);
                long count = IntegrationService.countIntegrations(query, true, ctx);
                if (count > 0) result = false;
            }

            if (! result) {
                String data = String.format("{\"applicationId\":\"%s\",\"action\":\"%s\",\"reason\":\"%s\"}", id, action, reason);
                resp.setBody(ErrorResponse.compose("OP_ERR", data));
            }
        }
        else
            resp.setBody(ErrorResponse.compose("NOT_FOUND", "{\"applicationId\":\""+id+"\"}"));

        return result;
    }

}
