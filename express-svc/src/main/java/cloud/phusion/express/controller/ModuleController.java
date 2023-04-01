package cloud.phusion.express.controller;

import cloud.phusion.Context;
import cloud.phusion.DataObject;
import cloud.phusion.Engine;
import cloud.phusion.PhusionException;
import cloud.phusion.express.service.*;
import cloud.phusion.express.util.*;
import cloud.phusion.protocol.http.HttpMethod;
import cloud.phusion.protocol.http.HttpRequest;
import cloud.phusion.protocol.http.HttpResponse;
import com.alibaba.fastjson2.JSONObject;

import java.util.HashMap;
import java.util.Map;

public class ModuleController {
    private static final String thisCategory = "module";

    public static void dispatch(JSONObject currentUser, HttpRequest req, HttpResponse resp, Context ctx) throws Exception {
        String item = req.getParameter("_item");
        String action = req.getMethod().toString();
        String countOnly;

        if (item == null) action += " null";
        else action += " "+item;

        switch (action) {
            case "PUT java": _saveJavaModule(currentUser, req, resp, ctx); break;
            case "POST java": _saveJar(currentUser, req, resp, ctx); break;
            case "POST nodejs": _installNodeJS(currentUser, req, resp, ctx); break;
            case "POST code": _saveAndRunCode(currentUser, req, resp, ctx); break;
            case "GET java": _getJavaModule(currentUser, req, resp, ctx); break;
            case "GET nodejs": _listNodeJSModules(currentUser, req, resp, ctx); break;
            case "GET code": _getCode(currentUser, req, resp, ctx); break; // For role privilege, grant "GET module listallcode" to list all person's code
            case "GET id": _getId(currentUser, req, resp, ctx); break;
            case "GET encode": _encode(currentUser, true, req, resp, ctx); break;
            case "GET decode": _encode(currentUser, false, req, resp, ctx); break;
            case "DELETE java": _removeJar(currentUser, req, resp, ctx); break;
            case "DELETE nodejs": _uninstallNodeJS(currentUser, req, resp, ctx); break;
            case "DELETE code": _removeCode(currentUser, req, resp, ctx); break;
            default: resp.setBody(ErrorResponse.compose("BAD_REQ_URL"));
        }
    }

    private static void _getJavaModule(JSONObject currentUser, HttpRequest req, HttpResponse resp, Context ctx) throws Exception {
        String moduleId = req.getParameter("module");
        if (moduleId!=null && moduleId.length()==0) moduleId = null;

        if (! CommonHandler.checkAuthorizationResult(
                AuthorizationService.checkPrivilege(currentUser, HttpMethod.GET, thisCategory, "java", null, ctx),
                resp)) return;

        DataObject result;

        if (moduleId == null) {
            String noSystemStr = req.getParameter("noSystemModule");
            boolean noSystem = "true".equals(noSystemStr);
            result = ModuleService.listJavaModules(noSystem, ctx);
        }
        else {
            if (moduleId.equals(ModuleService.JAVAMODUEL_SYSTSEM))
                result = ModuleService.fetchSystemJavaModule(ctx);
            else
                result = ModuleService.fetchJavaModule(moduleId, true, ctx);

            if (result == null) {
                resp.setBody(ErrorResponse.compose("NOT_FOUND", "{\"module\":\""+moduleId+"\"}"));
                return;
            }
        }

        resp.setBody(new DataObject("{\"result\":"+result.getString()+"}"));
    }

    private static String[] _systemKeyword = new String[]{ModuleService.JAVAMODUEL_SYSTSEM};

    private static void _saveJavaModule(JSONObject currentUser, HttpRequest req, HttpResponse resp, Context ctx) throws Exception {
        JSONObject body = CommonHandler.parseDataToJSONObject(req.getBody(), resp, "BAD_REQ_BODY", ctx);
        if (body == null) return;

        if (! CommonHandler.checkAuthorizationResult(
                AuthorizationService.checkPrivilege(currentUser, HttpMethod.PUT, thisCategory, "java", null, ctx),
                resp)) return;

        if (! CommonHandler.checkExistence(body, new String[]{"module"}, resp)) return;
        if (! CommonHandler.checkImmutable(body, new String[]{"jars"}, resp)) return;

        String moduleId = body.getString("module");
        if (! CommonHandler.checkModulePattern(moduleId, "module", resp)) return;
        if (! CommonHandler.checkNotKeywords(moduleId, _systemKeyword, "module", resp)) return;

        DataObject moduleDisk = ModuleService.fetchJavaModule(moduleId, false, ctx);
        JSONObject moduleObj = moduleDisk==null ? null : CommonCode.parseDataToJSONObject(moduleDisk, ctx);

        JSONObject moduleNew = CommonHandler.mergeJSONObjects(body, moduleObj,
                new String[]{"module","packages"},
                null,
                resp);
        if (moduleNew == null) return;

        ModuleService.saveJavaModule(moduleId, new DataObject(moduleNew), ctx);
        resp.setBody(new DataObject("{}"));
    }

    private static void _saveJar(JSONObject currentUser, HttpRequest req, HttpResponse resp, Context ctx) throws Exception {
        if (! CommonHandler.checkAuthorizationResult(
                AuthorizationService.checkPrivilege(currentUser, HttpMethod.POST, thisCategory, "java", null, ctx),
                resp)) {
            CommonHandler.clearFiles(req);
            return;
        }

        if (! CommonHandler.checkParamExistence(req, new String[]{"module"}, resp)) {
            CommonHandler.clearFiles(req);
            return;
        }

        String moduleId = req.getParameter("module");
        if (! CommonHandler.checkModulePattern(moduleId, "module", resp)) {
            CommonHandler.clearFiles(req);
            return;
        }
        if (! CommonHandler.checkNotKeywords(moduleId, _systemKeyword, "module", resp)) {
            CommonHandler.clearFiles(req);
            return;
        }

        ModuleService.uploadJars(moduleId, req, ctx);

        resp.setBody(new DataObject("{}"));
    }

    private static void _removeJar(JSONObject currentUser, HttpRequest req, HttpResponse resp, Context ctx) throws Exception {
        if (! CommonHandler.checkParamExistence(req, new String[]{"module"}, resp)) return;

        if (! CommonHandler.checkAuthorizationResult(
                AuthorizationService.checkPrivilege(currentUser, HttpMethod.DELETE, thisCategory, "java", null, ctx),
                resp)) return;

        String moduleId = req.getParameter("module");
        if (! CommonHandler.checkNotKeywords(moduleId, _systemKeyword, "module", resp)) return;

        String jar = req.getParameter("jar");

        try {
            if (! _checkModuleCanBeRemoved("jar", moduleId, jar, resp, ctx)) return;

            if (jar==null || jar.length()==0) ModuleService.removeJavaModule(moduleId, ctx);
            else ModuleService.removeJar(moduleId, jar, ctx);
        } catch (PhusionException ex) {
            String code = ex.getCode();
            switch (code) {
                case "FS_OP":
                    String reason = "Java module is in use, or does not exist";
                    String data = String.format("{\"module\":\"%s\",\"jar\":\"%s\",\"action\":\"remove\",\"reason\":\"%s\"}",
                            moduleId, (jar==null || jar.length()==0)?"all":jar, reason);
                    resp.setBody(ErrorResponse.compose("OP_ERR", data));
                    return;
                default:
                    throw ex;
            }
        }

        resp.setBody(new DataObject("{}"));
    }

    private static void _listNodeJSModules(JSONObject currentUser, HttpRequest req, HttpResponse resp, Context ctx) throws Exception {
        if (! CommonHandler.checkAuthorizationResult(
                AuthorizationService.checkPrivilege(currentUser, HttpMethod.GET, thisCategory, "nodejs", null, ctx),
                resp)) return;

        DataObject result = ModuleService.listNodeJSModules(ctx);
        resp.setBody(new DataObject("{\"result\":"+result.getString()+"}"));
    }

    private static void _installNodeJS(JSONObject currentUser, HttpRequest req, HttpResponse resp, Context ctx) throws Exception {
        if (! CommonHandler.checkParamExistence(req, new String[]{"module"}, resp)) return;

        if (! CommonHandler.checkAuthorizationResult(
                AuthorizationService.checkPrivilege(currentUser, HttpMethod.POST, thisCategory, "nodejs", null, ctx),
                resp)) return;

        String module = req.getParameter("module");
        boolean success = ModuleService.installNodeJSModule(module, ctx);

        if (success) resp.setBody(new DataObject("{}"));
        else {
            String reason = "Installation failed";
            String data = String.format("{\"module\":\"%s\",\"action\":\"install\",\"reason\":\"%s\"}", module, reason);
            resp.setBody(ErrorResponse.compose("OP_ERR", data));
        }
    }

    private static void _uninstallNodeJS(JSONObject currentUser, HttpRequest req, HttpResponse resp, Context ctx) throws Exception {
        if (! CommonHandler.checkParamExistence(req, new String[]{"module"}, resp)) return;

        if (! CommonHandler.checkAuthorizationResult(
                AuthorizationService.checkPrivilege(currentUser, HttpMethod.DELETE, thisCategory, "nodejs", null, ctx),
                resp)) return;

        String module = req.getParameter("module");
        boolean success = ModuleService.uninstallNodeJSModule(module, ctx);

        if (success) resp.setBody(new DataObject("{}"));
        else {
            String reason = "Uninstallation failed";
            String data = String.format("{\"module\":\"%s\",\"action\":\"uninstall\",\"reason\":\"%s\"}", module, reason);
            resp.setBody(ErrorResponse.compose("OP_ERR", data));
        }
    }

    private static String[] _integrationKeyword = new String[]{ModuleService.CODE_OWNER_INTEGRATION};

    private static void _saveAndRunCode(JSONObject currentUser, HttpRequest req, HttpResponse resp, Context ctx) throws Exception {
        String type = req.getParameter("type");
        String filename = req.getParameter("filename");
        String asyncStr = req.getParameter("async");
        boolean async = "true".equals(asyncStr);
        String saveOnlyStr = req.getParameter("saveOnly");
        boolean saveOnly = "true".equals(saveOnlyStr);
        String timespanStr = req.getParameter("returnTimespan");
        boolean needTimespan = "true".equals(timespanStr);
        String owner = req.getParameter("owner");
        DataObject bodyObj = req.getBody();
        String body = bodyObj==null ? null : bodyObj.getString();

        if (type!=null && type.length()==0) type = null;
        if (filename!=null && filename.length()==0) filename = null;
        if (body!=null && body.length()==0) body = null;
        if (owner!=null && owner.length()==0) owner = null;

        if (owner==null && currentUser!=null) {
            owner = currentUser.getString("id");
            if (owner!=null && owner.length()==0) owner = null;
        }

        if (type!=null && filename!=null && ! type.equals(CodeRunner.getTypeFromFilename(filename))) {
            String data = String.format("{\"type\":\"%s\",\"filename\":\"%s\",\"exception\":\"Type not matched with the suffix of the filename\"}",
                    type, filename);
            resp.setBody(ErrorResponse.compose("BAD_REQ_PARAM", data));
            return;
        }

        if (type == null)
            type = filename==null ? CodeRunner.CODE_TYPE_JS : CodeRunner.getTypeFromFilename(filename);

        if (type.equals(CodeRunner.CODE_TYPE_NODE) || type.equals(CodeRunner.CODE_TYPE_DSL_I)) {
            if (filename == null) {
                String data = String.format("{\"exception\":\"For %s and %s, filename must be specified\"}",
                        CodeRunner.CODE_TYPE_NODE, CodeRunner.CODE_TYPE_DSL_I);
                resp.setBody(ErrorResponse.compose("BAD_REQ_PARAM", data));
                return;
            }
        }

        if (! CommonHandler.checkModulePattern(filename, "filename", resp)) return;

        String code = body==null ? null : CodeRunner.wrapCode(type, body);

        Map<String,Object> params = new HashMap<>();
        params.put("owner", owner);
        params.put("filename", filename);
        if (! CommonHandler.checkAuthorizationResult(
                AuthorizationService.checkPrivilege(currentUser, HttpMethod.POST, thisCategory, "code", params, ctx),
                resp)) return;

        if (saveOnly) {
            if (body == null) {
                resp.setBody(ErrorResponse.compose("BAD_REQ_BODY"));
                return;
            }
            if (! CommonHandler.checkParamExistence(req, new String[]{"filename"}, resp)) return;

            ModuleService.saveCode(owner, filename, new DataObject(code), ctx);
            resp.setBody(new DataObject("{}"));
        }
        else {
            resp.setHeader("Content-Type", "text/plain; charset=UTF-8");
            boolean reload = true;

            if (code != null) {
                if (filename != null) ModuleService.saveCode(owner, filename, new DataObject(code), ctx);
            }
            else {
                if (! ModuleService.hasCode(owner, filename, ctx)) {
                    String data = String.format("{\"owner\":\"%s\",\"filename\":\"%s\"}", owner, filename);
                    resp.setBody(ErrorResponse.compose("NOT_FOUND", data));
                    return;
                }
                reload = false;
            }

            TimeMarker t = new TimeMarker();

            String result = CodeRunner.runCode(type, code, owner, filename, async, reload, ctx);

            if (needTimespan) {
                if (result == null) result = "";
                result += String.format("\n\nExecution time: %.2fms", t.mark());
            }

            resp.setBody((new DataObject(result)));
        }
    }

    private static void _getCode(JSONObject currentUser, HttpRequest req, HttpResponse resp, Context ctx) throws Exception {
        String filename = req.getParameter("filename");
        String owner = req.getParameter("owner");

        if (filename!=null && filename.length()==0) filename = null;
        if (owner!=null && owner.length()==0) owner = null;

        if (owner==null && currentUser!=null) {
            owner = currentUser.getString("id");
            if (owner!=null && owner.length()==0) owner = null;
        }

        Map<String,Object> params = new HashMap<>();
        params.put("owner", owner);
        params.put("filename", filename);
        if (! CommonHandler.checkAuthorizationResult(
                AuthorizationService.checkPrivilege(currentUser, HttpMethod.GET, thisCategory, "code", params, ctx),
                resp)) return;

        if (filename == null) {
            if (!CommonHandler.checkNotKeywords(owner, _integrationKeyword, "owner", resp)) return;

            DataObject result;
            if (owner.equals("all")) result = ModuleService.listCodes(ctx);
            else result = ModuleService.listCodes(owner, ctx);

            resp.setBody(new DataObject("{\"result\":"+result.getString()+"}"));
        }
        else {
            DataObject file = ModuleService.fetchCode(owner, filename, ctx);
            String type = CodeRunner.getTypeFromFilename(filename);
            String code = file==null ? "" : CodeRunner.unwrapCode(type, file.getString());

            resp.setHeader("Content-Type", "text/plain; charset=UTF-8");
            resp.setBody(new DataObject(code));
        }
    }

    private static void _removeCode(JSONObject currentUser, HttpRequest req, HttpResponse resp, Context ctx) throws Exception {
        if (! CommonHandler.checkParamExistence(req, new String[]{"filename"}, resp)) return;

        String filename = req.getParameter("filename");
        String owner = req.getParameter("owner");
        if (! CommonHandler.checkNotKeywords(owner, _integrationKeyword, "owner", resp)) return;

        if (owner!=null && owner.length()==0) owner = null;

        if (owner==null && currentUser!=null) {
            owner = currentUser.getString("id");
            if (owner!=null && owner.length()==0) owner = null;
        }

        Map<String,Object> params = new HashMap<>();
        params.put("owner", owner);
        params.put("filename", filename);
        if (! CommonHandler.checkAuthorizationResult(
                AuthorizationService.checkPrivilege(currentUser, HttpMethod.DELETE, thisCategory, "code", params, ctx),
                resp)) return;

        if (! _checkModuleCanBeRemoved("code", owner, filename, resp, ctx)) return;
        ModuleService.removeCode(owner, filename, ctx);

        resp.setBody(new DataObject("{}"));
    }

    private static void _getId(JSONObject currentUser, HttpRequest req, HttpResponse resp, Context ctx) throws Exception {
        String countStr = req.getParameter("count");
        int count = (countStr==null || countStr.length()==0) ? 1 : Integer.parseInt(countStr);

        if (! CommonHandler.checkAuthorizationResult(
                AuthorizationService.checkPrivilege(currentUser, HttpMethod.GET, thisCategory, "id", null, ctx),
                resp)) return;

        StringBuilder result = new StringBuilder();
        Engine engine = ctx.getEngine();
        result.append("{\"result\":[");
        for (int i = 0; i < count; i++) {
            if (i > 0) result.append(',');
            result.append(engine.generateUniqueId(ctx));
        }
        result.append("]}");

        resp.setBody(new DataObject(result.toString()));
    }

    private static void _encode(JSONObject currentUser, boolean encoding, HttpRequest req, HttpResponse resp, Context ctx) throws Exception {
        boolean asQuery = "true".equals(req.getParameter("asQuery"));

        if (! CommonHandler.checkAuthorizationResult(
                AuthorizationService.checkPrivilege(currentUser, HttpMethod.GET, thisCategory, "encode", null, ctx),
                resp)) return;

        DataObject body = req.getBody();
        String result = "";

        if (body != null) {
            String str = body.getString();
            if (str!=null && str.length()>0) {
                result = encoding ?
                        (asQuery ? FullTextEncoder.encodeAsQueryString(str) : FullTextEncoder.encode(str)) :
                        FullTextEncoder.decode(str);
            }
        }

        resp.setHeader("Content-Type", "text/plain; charset=UTF-8");
        resp.setBody(new DataObject(result));
    }

    private static boolean _checkModuleCanBeRemoved(String type, String parent, String file, HttpResponse resp, Context ctx) throws Exception {
        boolean result = false;
        String data;

        if (type.equals("code")) {
            data = "{\"owner\":\""+parent+"\",\"filename\":\""+file+"\"}";
            result = ModuleService.hasCode(parent, file, ctx);
        }
        else if (file==null || file.length()==0) {
            data = "{\"module\":\""+parent+"\"}";
            result = ModuleService.hasJavaModule(parent, ctx);
        }
        else {
            data = "{\"module\":\""+parent+"\",\"jar\":\""+file+"\"}";
            result = ModuleService.hasJar(parent, file, ctx);
        }

        if (! result) resp.setBody(ErrorResponse.compose("NOT_FOUND", data));

        return result;
    }

}
