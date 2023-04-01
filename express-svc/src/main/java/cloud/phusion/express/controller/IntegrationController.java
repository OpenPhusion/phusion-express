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

public class IntegrationController {
    private static final String thisCategory = "integration";

    private static final Set<String> predefinedKeywords = new HashSet<>(
            Arrays.asList("table", "operation", "category", "template", "test")
    );

    public static void dispatch(JSONObject currentUser, HttpRequest req, HttpResponse resp, Context ctx) throws Exception {
        String item = req.getParameter("_item");
        String action = req.getMethod().toString();
        String countOnly;

        if (item == null) action += " null";
        else if (predefinedKeywords.contains(item)) action += " "+item;
        else action += " id";

        switch (action) {
            case "PUT null": _saveIntegration(currentUser, req, resp, ctx); break;
            case "PUT table": _saveDBTable(currentUser, req, resp, ctx); break;
            case "POST operation": _operateIntegration(currentUser, req, resp, ctx); break;
            case "POST test": _testIntegration(currentUser, req, resp, ctx); break;
            case "GET null":
                countOnly = req.getParameter("countOnly");
                if ("true".equals(countOnly)) _countIntegrations(currentUser, req, resp, ctx);
                else _listIntegrations(currentUser, req, resp, ctx);
                break;
            case "GET id": _getIntegration(currentUser, item, req, resp, ctx); break;
            case "GET table": _getDBTable(currentUser, req, resp, ctx); break;
            case "GET category": _listCategories(currentUser, req, resp, ctx); break;
            case "GET template": _listTemplates(currentUser, req, resp, ctx); break;
            case "DELETE table": _removeDBTable(currentUser, req, resp, ctx); break;
            case "DELETE id": _removeIntegration(currentUser, item, req, resp, ctx); break;
            default: resp.setBody(ErrorResponse.compose("BAD_REQ_URL"));
        }
    }

    private static void _saveIntegration(JSONObject currentUser, HttpRequest req, HttpResponse resp, Context ctx) throws Exception {
        JSONObject body = CommonHandler.parseDataToJSONObject(req.getBody(), resp, "BAD_REQ_BODY", ctx);
        if (body == null) return;

        if (! CommonHandler.checkExistence(body, new String[]{"id"}, resp)) return;
        if (! CommonHandler.checkImmutable(body, new String[]{"status","tables","createTime","updateTime"}, resp)) return;

        String id = body.getString("id");
        if (! CommonHandler.checkIDPattern(id, "id", resp)) return;

        boolean categoryChanged = body.containsKey("categories");
        boolean workflowChanged = body.containsKey("workflow");
        boolean applicationChanged = body.containsKey("applications");
        boolean generateScript = "true".equals(req.getParameter("generateScript"));
        String template = body.getString("template");
        if (template!=null && template.length()==0) template = null;

        DataObject itDisk = IntegrationService.fetchIntegration(id, ctx);
        JSONObject itObj = itDisk==null ? null : CommonCode.parseDataToJSONObject(itDisk, ctx);

        IntegrationService.saveIntegrationSchemasFromObject(id, body, ctx);

        JSONObject itNew = CommonHandler.mergeJSONObjects(body, itObj,
                new String[]{"id","autoStart","abstract","title","clientId","applications","categories","workflow",
                        "template","startCondition","timer","desc"},
                new String[]{"config"},
                resp);
        if (itNew == null) return;

        if (itNew.getBooleanValue("abstract", false)) {
            itNew.put("autoStart", false);
        }

        if (template != null) {
            boolean isAbstract = itNew.getBooleanValue("abstract", false);
            if (isAbstract || ! IntegrationService.isTemplate(template, ctx)) {
                resp.setBody(ErrorResponse.compose("BAD_REQ_PARAM", "{\"exception\":\"The template must be abstract, and its user must not be abstract\"}"));
                return;
            }

//            itNew.remove("timer");
//            itNew.remove("workflow");
        }

        Map<String,Object> params = new HashMap<>();
        params.put("id", id);
        params.put("clientId", itNew.getString("clientId"));
        if (! CommonHandler.checkAuthorizationResult(
                AuthorizationService.checkPrivilege(currentUser, HttpMethod.PUT, thisCategory, null, params, ctx),
                resp)) return;

        IntegrationService.saveIntegration(id, new DataObject(itNew), workflowChanged, categoryChanged, applicationChanged, generateScript, null, ctx);
        resp.setBody(new DataObject("{}"));
    }

    private static void _testIntegration(JSONObject currentUser, HttpRequest req, HttpResponse resp, Context ctx) throws Exception {
        JSONObject body = CommonHandler.parseDataToJSONObject(req.getBody(), resp, "BAD_REQ_BODY", ctx);
        if (body == null) return;

        if (! CommonHandler.checkExistence(body, new String[]{"step","integrationId","msg"}, resp)) return;

        String id = body.getString("integrationId");
        if (! _checkIntegration(id, null, "test", null, resp, ctx)) return;

        String step = body.getString("step");
        if (id!=null && id.length()==0) id = null;
        if (step!=null && step.length()==0) step = null;

        Map<String,Object> params = new HashMap<>();
        params.put("id", id);
        if (! CommonHandler.checkAuthorizationResult(
                AuthorizationService.checkPrivilege(currentUser, HttpMethod.POST, thisCategory, "test", params, ctx),
                resp)) return;

        Object msg = body.get("msg");

        JSONObject properties = body.getJSONObject("properties");
        if (properties!=null && properties.size()==0) properties = null;

        boolean failed = body.getBooleanValue("failed", false);
        boolean oneStepOnly = body.getBooleanValue("oneStepOnly", false);

        DataObject testResult = IntegrationService.testIntegration(
                id, step, failed,
                msg==null ? null : new DataObject(msg),
                properties==null ? null : new DataObject(properties),
                ! oneStepOnly, ctx
        );

        JSONObject result = new JSONObject();
        result.put("result", testResult==null ? null : testResult.getJSONObject());
        resp.setBody(new DataObject(result));
    }

    private static void _saveDBTable(JSONObject currentUser, HttpRequest req, HttpResponse resp, Context ctx) throws Exception {
        JSONObject body = CommonHandler.parseDataToJSONObject(req.getBody(), resp, "BAD_REQ_BODY", ctx);
        if (body == null) return;

        if (! CommonHandler.checkExistence(body, new String[]{"name","integrationId"}, resp)) return;

        String tableId = body.getString("name");
        if (! CommonHandler.checkIDPattern(tableId, "name", resp)) return;

        String integrationId = body.getString("integrationId");
        body.remove("integrationId");

        JSONObject itObj = _getIntegrationFromDisk(integrationId, resp, ctx);
        if (itObj == null) return;

        Map<String,Object> params = new HashMap<>();
        params.put("integrationId", integrationId);
        params.put("clientId", itObj.getString("clientId"));
        if (! CommonHandler.checkAuthorizationResult(
                AuthorizationService.checkPrivilege(currentUser, HttpMethod.PUT, thisCategory, "table", params, ctx),
                resp)) return;

        JSONObject tableObj = CommonCode.findItemInJSONArray(itObj.getJSONArray("tables"), "name", tableId);
        JSONObject tableNew = CommonHandler.mergeJSONObjects(body, tableObj,
                new String[]{"name","fields","desc","indexes"},
                null,
                resp);
        if (tableNew == null) return;

        CommonCode.insertItemIntoJSONArray(itObj, "tables", "name", tableNew);

        IntegrationService.saveIntegration(integrationId, new DataObject(itObj), false, false, false, false, tableId, ctx);
        resp.setBody(new DataObject("{}"));
    }

    private static void _getDBTable(JSONObject currentUser, HttpRequest req, HttpResponse resp, Context ctx) throws Exception {
        JSONObject body = CommonHandler.getParamsFromBodyOrParams(req, new String[]{"name","integrationId"}, resp, ctx);
        if (body == null) return;

        String tableId = body.getString("name");
        String integrationId = body.getString("integrationId");

        JSONObject itObj = _getIntegrationFromDisk(integrationId, resp, ctx);
        if (itObj == null) return;

        Map<String,Object> params = new HashMap<>();
        params.put("integrationId", integrationId);
        params.put("clientId", itObj.getString("clientId"));
        if (! CommonHandler.checkAuthorizationResult(
                AuthorizationService.checkPrivilege(currentUser, HttpMethod.GET, thisCategory, "table", params, ctx),
                resp)) return;

        JSONArray arr = itObj.getJSONArray("tables");
        String data = "{\"integrationId\":\""+integrationId+"\",\"tableName\":\""+tableId+"\"}";
        int pos = CommonHandler.findItemInJSONArray(arr, "name", tableId, data, resp);
        if (pos < 0) return;

        JSONObject result = new JSONObject();
        result.put("result", arr.getJSONObject(pos));
        resp.setBody(new DataObject(result));
    }

    private static void _removeDBTable(JSONObject currentUser, HttpRequest req, HttpResponse resp, Context ctx) throws Exception {
        if (! CommonHandler.checkParamExistence(req, new String[]{"name","integrationId"}, resp)) return;

        String tableId = req.getParameter("name");
        String integrationId = req.getParameter("integrationId");

        JSONObject itObj = _getIntegrationFromDisk(integrationId, resp, ctx);
        if (itObj == null) return;

        Map<String,Object> params = new HashMap<>();
        params.put("integrationId", integrationId);
        params.put("clientId", itObj.getString("clientId"));
        if (! CommonHandler.checkAuthorizationResult(
                AuthorizationService.checkPrivilege(currentUser, HttpMethod.DELETE, thisCategory, "table", params, ctx),
                resp)) return;

        JSONArray arr = itObj.getJSONArray("tables");
        String data = "{\"integrationId\":\""+integrationId+"\",\"tableName\":\""+tableId+"\"}";
        int pos = CommonHandler.findItemInJSONArray(arr, "name", tableId, data, resp);
        if (pos < 0) return;

        arr.remove(pos);
        IntegrationService.saveIntegration(integrationId, new DataObject(itObj), false, false, false, false, null, ctx);

        // Remove table physically

        boolean physical = "true".equals(req.getParameter("removePhysically"));
        if (physical) CommonCode.removeTablePhysically("integration", integrationId, tableId, ctx);

        resp.setBody(new DataObject("{}"));
    }

    private static void _countIntegrations(JSONObject currentUser, HttpRequest req, HttpResponse resp, Context ctx) throws Exception {
        String search = req.getParameter("search");
        String template = req.getParameter("template");
        String category = req.getParameter("category");
        String clientId = req.getParameter("clientId");
        String applicationId = req.getParameter("applicationId");
        String connectionId = req.getParameter("connectionId");
        String abstractStr = req.getParameter("abstract");
        if (search!=null && search.length()==0) search = null;
        if (template!=null && template.length()==0) template = null;
        if (category!=null && category.length()==0) category = null;
        if (clientId!=null && clientId.length()==0) clientId = null;
        if (applicationId!=null && applicationId.length()==0) applicationId = null;
        if (connectionId!=null && connectionId.length()==0) connectionId = null;
        if (abstractStr!=null && abstractStr.length()==0) abstractStr = null;

        if (clientId==null && currentUser!=null) {
            clientId = currentUser.getString("clientId");
            if (clientId!=null && clientId.length()==0) clientId = null;
        }

        Map<String,Object> params = new HashMap<>();
        params.put("clientId", clientId);
        if (! CommonHandler.checkAuthorizationResult(
                AuthorizationService.checkPrivilege(currentUser, HttpMethod.GET, thisCategory, null, params, ctx),
                resp)) return;

        Map<String,Object> query = new HashMap<>();
        query.put("search", search);
        query.put("category", category);
        query.put("clientId", clientId);
        query.put("applicationId", applicationId);
        query.put("connectionId", connectionId);
        query.put("template", template);
        query.put("abstractStr", abstractStr);

        long count = IntegrationService.countIntegrations(query, ctx);
        resp.setBody(new DataObject("{\"result\":"+count+"}"));
    }

    private static void _listIntegrations(JSONObject currentUser, HttpRequest req, HttpResponse resp, Context ctx) throws Exception {
        String ids = req.getParameter("ids");
        String fields = req.getParameter("fields");
        if (fields!=null && fields.length()==0) fields = null;

        DataObject result;

        if (ids==null || ids.length()==0) {
            String search = req.getParameter("search");
            String template = req.getParameter("template");
            String category = req.getParameter("category");
            String clientId = req.getParameter("clientId");
            String applicationId = req.getParameter("applicationId");
            String connectionId = req.getParameter("connectionId");
            String abstractStr = req.getParameter("abstract");
            if (search!=null && search.length()==0) search = null;
            if (template!=null && template.length()==0) template = null;
            if (category!=null && category.length()==0) category = null;
            if (clientId!=null && clientId.length()==0) clientId = null;
            if (applicationId!=null && applicationId.length()==0) applicationId = null;
            if (connectionId!=null && connectionId.length()==0) connectionId = null;
            if (abstractStr!=null && abstractStr.length()==0) abstractStr = null;

            String fromStr = req.getParameter("from");
            String lengthStr = req.getParameter("length");
            long from = (fromStr == null || fromStr.length() == 0) ? 0 : Long.parseLong(fromStr);
            long length = (lengthStr == null || lengthStr.length() == 0) ? 100 : Long.parseLong(lengthStr);

            if (clientId==null && currentUser!=null) {
                clientId = currentUser.getString("clientId");
                if (clientId!=null && clientId.length()==0) clientId = null;
            }

            Map<String,Object> params = new HashMap<>();
            params.put("clientId", clientId);
            if (! CommonHandler.checkAuthorizationResult(
                    AuthorizationService.checkPrivilege(currentUser, HttpMethod.GET, thisCategory, null, params, ctx),
                    resp)) return;

            Map<String,Object> query = new HashMap<>();
            query.put("search", search);
            query.put("category", category);
            query.put("clientId", clientId);
            query.put("applicationId", applicationId);
            query.put("connectionId", connectionId);
            query.put("template", template);
            query.put("abstractStr", abstractStr);

            result = IntegrationService.queryIntegrations(query, fields, from, length, ctx);
        }
        else {
            result = IntegrationService.listIntegrationsById(ids, fields, ctx);

            Map<String,Object> params = new HashMap<>();
            params.put("integrationIds", CommonCode.getValueSetFromJSONArray(result, "id"));
            params.put("clientId", CommonCode.getCommonValueFromJSONArray(result,"clientId"));
            if (! CommonHandler.checkAuthorizationResult(
                    AuthorizationService.checkPrivilege(currentUser, HttpMethod.GET, thisCategory, null, params, ctx),
                    resp)) return;
        }

        resp.setBody(new DataObject("{\"result\":"+result.getString()+"}"));
    }

    private static void _listCategories(JSONObject currentUser, HttpRequest req, HttpResponse resp, Context ctx) throws Exception {
        if (! CommonHandler.checkAuthorizationResult(
                AuthorizationService.checkPrivilege(currentUser, HttpMethod.GET, thisCategory, "category", null, ctx),
                resp)) return;

        DataObject result = IntegrationService.listCategories(ctx);
        resp.setBody(new DataObject("{\"result\":"+result.getString()+"}"));
    }

    private static void _listTemplates(JSONObject currentUser, HttpRequest req, HttpResponse resp, Context ctx) throws Exception {
        if (! CommonHandler.checkAuthorizationResult(
                AuthorizationService.checkPrivilege(currentUser, HttpMethod.GET, thisCategory, "template", null, ctx),
                resp)) return;

        DataObject result = IntegrationService.listTemplates(ctx);
        resp.setBody(new DataObject("{\"result\":"+result.getString()+"}"));
    }

    private static void _getIntegration(JSONObject currentUser, String id, HttpRequest req, HttpResponse resp, Context ctx) throws Exception {
        JSONObject itObj = _getIntegrationFromDisk(id, resp, ctx);
        if (itObj == null) return;

        Map<String,Object> params = new HashMap<>();
        params.put("id", id);
        params.put("clientId", itObj.getString("clientId"));
        if (! CommonHandler.checkAuthorizationResult(
                AuthorizationService.checkPrivilege(currentUser, HttpMethod.GET, thisCategory, "id", params, ctx),
                resp)) return;

        String hideSchemaStr = req.getParameter("hideSchemas");
        boolean hideSchema = "true".equals(hideSchemaStr);
        if (! hideSchema) IntegrationService.fetchSchemas(itObj, ctx);

        String status = IntegrationService.getIntegrationLocalStatus(id, ctx);
        itObj.put("status", status);

        JSONObject result = new JSONObject();
        result.put("result", itObj);
        resp.setBody(new DataObject(result));
    }

    private static void _operateIntegration(JSONObject currentUser, HttpRequest req, HttpResponse resp, Context ctx) throws Exception {
        JSONObject body = CommonHandler.parseDataToJSONObject(req.getBody(), resp, "BAD_REQ_BODY", ctx);
        if (body == null) return;

        if (! CommonHandler.checkExistence(body, new String[]{"action","integrationId"}, resp)) return;

        String action = body.getString("action");
        String id = body.getString("integrationId");
        boolean local = body.getBooleanValue("local", false);

        Map<String,Object> params = new HashMap<>();
        params.put("id", id);
        if (! CommonHandler.checkAuthorizationResult(
                AuthorizationService.checkPrivilege(currentUser, HttpMethod.POST, thisCategory, "operation", params, ctx),
                resp)) return;

        if (action.equals(IntegrationService.ACTION_CONFIG))
            IntegrationService.saveConfig(id, body.getJSONObject("data"), ctx);
        else if (action.equals(IntegrationService.ACTION_MSG))
            IntegrationService.saveStepMessages(id, body.getJSONObject("data"), ctx);

        if (local) {
            String status = null;
            String reason = null;

            switch (action) {
                case IntegrationService.ACTION_START:
                    status = "!" + IntegrationService.STATUS_RUN;
                    reason = "Integration is running";
                    if (!_checkIntegration(id, status, action, reason, resp, ctx)) return;
                    IntegrationService.startIntegration(id, ctx);
                    break;
                case IntegrationService.ACTION_STOP:
                    status = IntegrationService.STATUS_RUN;
                    reason = "Integration is not running";
                    if (!_checkIntegration(id, status, action, reason, resp, ctx)) return;
                    IntegrationService.stopIntegration(id, ctx);
                    break;
                case IntegrationService.ACTION_RESTART:
                    if (!_checkIntegration(id, status, action, reason, resp, ctx)) return;
                    IntegrationService.restartIntegration(id, ctx);
                    break;
                case IntegrationService.ACTION_CONFIG:
                    status = IntegrationService.STATUS_RUN;
                    reason = "Integration is not running";
                    if (!_checkIntegration(id, status, action, reason, resp, ctx)) return;
                    IntegrationService.dynamicUpdateConfig(id, ctx);
                    break;
                case IntegrationService.ACTION_MSG:
                    status = IntegrationService.STATUS_RUN;
                    reason = "Integration is not running, or has a template";
                    if (!_checkIntegration(id, status, action, reason, resp, ctx)) return;
                    IntegrationService.dynamicUpdateStepMessages(id, ctx);
                    break;
                default:
                    resp.setBody(ErrorResponse.compose("OP_NONE", "{\"action\":\"" + action + "\"}"));
                    return;
            }
        }
        else {
            if (!_checkIntegration(id, null, action, "Integration has a template", resp, ctx)) return;

            if (action.equals(IntegrationService.ACTION_START) ||
                    action.equals(IntegrationService.ACTION_STOP) ||
                    action.equals(IntegrationService.ACTION_RESTART) ||
                    action.equals(IntegrationService.ACTION_CONFIG) ||
                    action.equals(IntegrationService.ACTION_MSG)) {
                ClusterService.sendMessage(ClusterService.OBJECT_INTEGRATION, action, id, ctx);
            }
            else {
                resp.setBody(ErrorResponse.compose("OP_NONE", "{\"action\":\"" + action + "\"}"));
                return;
            }
        }

        resp.setBody(new DataObject("{}"));
    }

    private static void _removeIntegration(JSONObject currentUser, String id, HttpRequest req, HttpResponse resp, Context ctx) throws Exception {
        Map<String,Object> params = new HashMap<>();
        params.put("id", id);
        if (! CommonHandler.checkAuthorizationResult(
                AuthorizationService.checkPrivilege(currentUser, HttpMethod.DELETE, thisCategory, "id", params, ctx),
                resp)) return;

        // Only check local status. To be optimized.
        if (! _checkIntegration(id, "!"+IntegrationService.STATUS_RUN, "remove", "Integration is running, or has associated integrations (through templates)", resp, ctx)) return;

        IntegrationService.removeIntegration(id, ctx);
        resp.setBody(new DataObject("{}"));
    }

    //----------------------------

    private static JSONObject _getIntegrationFromDisk(String id, HttpResponse resp, Context ctx) throws Exception {
        DataObject itDisk = IntegrationService.fetchIntegration(id, ctx);

        if (itDisk == null) {
            resp.setBody(ErrorResponse.compose("NOT_FOUND", "{\"integrationId\":\""+id+"\"}"));
            return null;
        }

        return CommonHandler.parseDataToJSONObject(itDisk, resp, "BAD_ENTITY", "integrationId", id, ctx);
    }

    private static boolean _checkIntegration(String id, String status, String action, String reason, HttpResponse resp, Context ctx) throws Exception {
        boolean result = false;

        if (IntegrationService.hasIntegration(id, ctx)) {
            if (status != null) {
                String statusIt = IntegrationService.getIntegrationLocalStatus(id, ctx);
                if (status.charAt(0) == '!') result = !statusIt.equals(status.substring(1));
                else result = statusIt.equals(status);
            }
            else result = true;

            if (action.equals("remove")) {
                // Check whether there are DB tables
                DataObject itDisk = IntegrationService.fetchIntegration(id, ctx);
                JSONObject itObj = itDisk.getJSONObject();
                JSONArray tables = itObj.getJSONArray("tables");
                if (tables!=null && tables.size()>0) result = false;

                // Check whether there are integrations associated with templates
                Map<String,Object> query = new HashMap<>();
                query.put("template", id);
                long count = IntegrationService.countIntegrations(query, ctx);
                if (count > 0) result = false;
            }
            else if (action.equals(IntegrationService.ACTION_MSG)) {
                // Check whether the integration has a template
                DataObject itDisk = IntegrationService.fetchIntegration(id, ctx);
                JSONObject itObj = itDisk.getJSONObject();
                String template = itObj.getString("template");
                if (template!=null && template.length()>0) result = false;
            }

            if (! result) {
                String data = String.format("{\"integrationId\":\"%s\",\"action\":\"%s\",\"reason\":\"%s\"}", id, action, reason);
                resp.setBody(ErrorResponse.compose("OP_ERR", data));
            }
        }
        else
            resp.setBody(ErrorResponse.compose("NOT_FOUND", "{\"integrationId\":\""+id+"\"}"));

        return result;
    }

}
