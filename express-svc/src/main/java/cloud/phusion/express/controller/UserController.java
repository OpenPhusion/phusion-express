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

public class UserController {
    private static final String thisCategory = "user";

    private static final Set<String> predefinedKeywords = new HashSet<>(
            Arrays.asList("role", "login", "permission")
    );

    public static void dispatch(JSONObject currentUser, HttpRequest req, HttpResponse resp, Context ctx) throws Exception {
        String item = req.getParameter("_item");
        String action = req.getMethod().toString();
        String countOnly;

        if (item == null) action += " null";
        else if (predefinedKeywords.contains(item)) action += " "+item;
        else action += " id";

        switch (action) {
            case "PUT null": _saveUser(currentUser, req, resp, ctx); break;
            case "PUT role": _saveRole(currentUser, req, resp, ctx); break;
            case "POST login": _login(currentUser, req, resp, ctx); break;
            case "GET null":
                countOnly = req.getParameter("countOnly");
                if ("true".equals(countOnly)) _countUsers(currentUser, req, resp, ctx);
                else _listUsers(currentUser, req, resp, ctx);
                break;
            case "GET id": _getUser(currentUser, item, req, resp, ctx); break;
            case "GET role": _listRoles(currentUser, req, resp, ctx); break;
            case "GET permission": _checkPermissions(currentUser, req, resp, ctx); break;
            case "DELETE login": _logout(currentUser, req, resp, ctx); break;
            case "DELETE role": _removeRole(currentUser, req, resp, ctx); break;
            case "DELETE id": _removeUser(currentUser, item, req, resp, ctx); break;
            default: resp.setBody(ErrorResponse.compose("BAD_REQ_URL"));
        }
    }

    private static void _saveRole(JSONObject currentUser, HttpRequest req, HttpResponse resp, Context ctx) throws Exception {
        JSONObject body = CommonHandler.parseDataToJSONObject(req.getBody(), resp, "BAD_REQ_BODY", ctx);
        if (body == null) return;

        if (! CommonHandler.checkExistence(body, new String[]{"id"}, resp)) return;

        String id = body.getString("id");
        if (! CommonHandler.checkIDPattern(id, "id", resp)) return;

        DataObject rolesDisk = UserService.listRolesFromDisk(ctx);
        JSONArray roles = rolesDisk==null ? new JSONArray() : rolesDisk.getJSONArray();

        JSONObject roleObj = CommonCode.findItemInJSONArray(roles, "id", id);
        JSONObject roleNew = CommonHandler.mergeJSONObjects(body, roleObj,
                new String[]{"id","title","grant","revoke","desc"},
                null, resp);
        if (roleNew == null) return;

        UserService.regulateRolePrivileges(roleNew.getJSONArray("grant"));
        UserService.regulateRolePrivileges(roleNew.getJSONArray("revoke"));

        if (roleObj != null) roles.remove(roleObj);
        roles.add(roleNew);

        if (! CommonHandler.checkAuthorizationResult(
                AuthorizationService.checkPrivilege(currentUser, HttpMethod.PUT, thisCategory, "role", null, ctx),
                resp)) return;

        UserService.saveRoles(new DataObject(roles), ctx);
        resp.setBody(new DataObject("{}"));
    }

    private static void _saveUser(JSONObject currentUser, HttpRequest req, HttpResponse resp, Context ctx) throws Exception {
        JSONObject body = CommonHandler.parseDataToJSONObject(req.getBody(), resp, "BAD_REQ_BODY", ctx);
        if (body == null) return;

        if (! CommonHandler.checkExistence(body, new String[]{"id"}, resp)) return;
        if (! CommonHandler.checkImmutable(body, new String[]{"createTime","updateTime"}, resp)) return;

        String id = body.getString("id");
        if (! CommonHandler.checkIDPattern(id, "id", resp)) return;

        boolean passwordChanged = body.containsKey("password");
        boolean permissionChanged = body.containsKey("permissions");
        boolean clientChanged = body.containsKey("clientId");

        DataObject userDB = UserService.fetchUser(id, null, ctx);
        JSONObject userObj = userDB==null ? null : CommonCode.parseDataToJSONObject(userDB, ctx);

        JSONObject userNew = CommonHandler.mergeJSONObjects(body, userObj,
                new String[]{"id","password","name","phone","email","icon","clientId","permissions","desc"},
                null, resp);
        if (userNew == null) return;

        Map<String,Object> params = new HashMap<>();
        params.put("id", userNew.getString("id"));
        params.put("clientId", userNew.getString("clientId"));
        if (permissionChanged) params.put("permissions", userNew.getJSONArray("permissions"));
        if (! CommonHandler.checkAuthorizationResult(
                AuthorizationService.checkPrivilege(currentUser, HttpMethod.PUT, thisCategory, null, params, ctx),
                resp)) return;

        UserService.saveUser(id, new DataObject(userNew), passwordChanged, clientChanged, permissionChanged, ctx);
        resp.setBody(new DataObject("{}"));
    }

    private static void _listRoles(JSONObject currentUser, HttpRequest req, HttpResponse resp, Context ctx) throws Exception {
        if (! CommonHandler.checkAuthorizationResult(
                AuthorizationService.checkPrivilege(currentUser, HttpMethod.GET, thisCategory, "role", null, ctx),
                resp)) return;

        DataObject result = UserService.listRolesFromDisk(ctx);
        if (result == null) resp.setBody(new DataObject("{}"));
        else resp.setBody(new DataObject("{\"result\":"+result.getString()+"}"));
    }

    private static void _checkPermissions(JSONObject currentUser, HttpRequest req, HttpResponse resp, Context ctx) throws Exception {
        JSONArray body = CommonHandler.parseDataToJSONArray(req.getBody(), resp, "BAD_REQ_BODY", ctx);
        if (body == null) return;

        DataObject result = AuthorizationService.checkPrivileges(currentUser, body, ctx);
        if (result == null) resp.setBody(new DataObject("{}"));
        else resp.setBody(new DataObject("{\"result\":"+result.getString()+"}"));
    }

    private static void _countUsers(JSONObject currentUser, HttpRequest req, HttpResponse resp, Context ctx) throws Exception {
        String search = req.getParameter("search");
        String phone = req.getParameter("phone");
        String email = req.getParameter("email");
        String clientId = req.getParameter("clientId");
        String roleId = req.getParameter("roleId");
        String scopeType = req.getParameter("scopeType");
        String scope = req.getParameter("scope");
        if (search!=null && search.length()==0) search = null;
        if (phone!=null && phone.length()==0) phone = null;
        if (email!=null && email.length()==0) email = null;
        if (clientId!=null && clientId.length()==0) clientId = null;
        if (roleId!=null && roleId.length()==0) roleId = null;
        if (scopeType!=null && scopeType.length()==0) scopeType = null;
        if (scope!=null && scope.length()==0) scope = null;

        if (clientId==null && currentUser!=null) {
            clientId = currentUser.getString("clientId");
            if (clientId!=null && clientId.length()==0) clientId = null;
        }

        Map<String,Object> query = new HashMap<>();
        query.put("search", search);
        query.put("phone", phone);
        query.put("email", email);
        query.put("clientId", clientId);
        query.put("roleId", roleId);
        query.put("scopeType", scopeType);
        query.put("scope", scope);

        Map<String,Object> params = new HashMap<>();
        params.put("clientId", clientId);
        params.put("fields", "id");
        if (! CommonHandler.checkAuthorizationResult(
                AuthorizationService.checkPrivilege(currentUser, HttpMethod.GET, thisCategory, null, params, ctx),
                resp)) return;

        long count = UserService.countUsers(query, ctx);
        resp.setBody(new DataObject("{\"result\":"+count+"}"));
    }

    private static void _listUsers(JSONObject currentUser, HttpRequest req, HttpResponse resp, Context ctx) throws Exception {
        String ids = req.getParameter("ids");
        String fields = req.getParameter("fields");
        if (fields!=null && fields.length()==0) fields = null;

        if (fields!=null && fields.contains("password")) {
            resp.setBody(ErrorResponse.compose("BAD_REQ_PARAM", "{\"exception\":\"Passwords can not be accessed\"}"));
            return;
        }

        DataObject result;

        if (ids==null || ids.length()==0) {
            String search = req.getParameter("search");
            String phone = req.getParameter("phone");
            String email = req.getParameter("email");
            String clientId = req.getParameter("clientId");
            String roleId = req.getParameter("roleId");
            String scopeType = req.getParameter("scopeType");
            String scope = req.getParameter("scope");
            if (search!=null && search.length()==0) search = null;
            if (phone!=null && phone.length()==0) phone = null;
            if (email!=null && email.length()==0) email = null;
            if (clientId!=null && clientId.length()==0) clientId = null;
            if (roleId!=null && roleId.length()==0) roleId = null;
            if (scopeType!=null && scopeType.length()==0) scopeType = null;
            if (scope!=null && scope.length()==0) scope = null;

            String fromStr = req.getParameter("from");
            String lengthStr = req.getParameter("length");
            long from = (fromStr == null || fromStr.length() == 0) ? 0 : Long.parseLong(fromStr);
            long length = (lengthStr == null || lengthStr.length() == 0) ? 100 : Long.parseLong(lengthStr);

            if (clientId==null && currentUser!=null) {
                clientId = currentUser.getString("clientId");
                if (clientId!=null && clientId.length()==0) clientId = null;
            }

            Map<String,Object> query = new HashMap<>();
            query.put("search", search);
            query.put("phone", phone);
            query.put("email", email);
            query.put("clientId", clientId);
            query.put("roleId", roleId);
            query.put("scopeType", scopeType);
            query.put("scope", scope);

            Map<String,Object> params = new HashMap<>();
            params.put("clientId", clientId);
            if (! CommonHandler.checkAuthorizationResult(
                    AuthorizationService.checkPrivilege(currentUser, HttpMethod.GET, thisCategory, null, params, ctx),
                    resp)) return;

            result = UserService.queryUsers(query, fields, from, length, ctx);
        }
        else {
            result = UserService.listUsersById(ids, fields, ctx);

            Map<String,Object> params = new HashMap<>();
            params.put("clientId", CommonCode.getCommonValueFromJSONArray(result,"clientId"));
            if (! CommonHandler.checkAuthorizationResult(
                    AuthorizationService.checkPrivilege(currentUser, HttpMethod.GET, thisCategory, null, params, ctx),
                    resp)) return;
        }

        resp.setBody(new DataObject("{\"result\":"+result.getString()+"}"));
    }

    private static void _getUser(JSONObject currentUser, String id, HttpRequest req, HttpResponse resp, Context ctx) throws Exception {
        if (id.equals("myself") && currentUser!=null) id = currentUser.getString("id");

        JSONObject userObj = _getUserFromDB(id, resp, ctx);
        if (userObj == null) return;

        Map<String,Object> params = new HashMap<>();
        params.put("id", id);
        params.put("clientId", userObj.getString("clientId"));
        if (! CommonHandler.checkAuthorizationResult(
                AuthorizationService.checkPrivilege(currentUser, HttpMethod.GET, thisCategory, "id", params, ctx),
                resp)) return;

        JSONObject result = new JSONObject();
        result.put("result", userObj);
        resp.setBody(new DataObject(result));
    }

    private static void _removeRole(JSONObject currentUser, HttpRequest req, HttpResponse resp, Context ctx) throws Exception {
        if (! CommonHandler.checkParamExistence(req, new String[]{"roleId"}, resp)) return;

        String roleId = req.getParameter("roleId");
        DataObject rolesDisk = UserService.listRolesFromDisk(ctx);
        JSONArray roles = rolesDisk==null ? new JSONArray() : rolesDisk.getJSONArray();

        String data = "{\"roleId\":\""+roleId+"\"}";
        int pos = CommonHandler.findItemInJSONArray(roles, "id", roleId, data, resp);
        if (pos < 0) return;

        roles.remove(pos);

        if (! CommonHandler.checkAuthorizationResult(
                AuthorizationService.checkPrivilege(currentUser, HttpMethod.DELETE, thisCategory, "role", null, ctx),
                resp)) return;

        UserService.saveRoles(new DataObject(roles), ctx);
        resp.setBody(new DataObject("{}"));
    }

    private static void _removeUser(JSONObject currentUser, String id, HttpRequest req, HttpResponse resp, Context ctx) throws Exception {
        if (! UserService.hasUser(id, ctx)) {
            resp.setBody(ErrorResponse.compose("NOT_FOUND", "{\"userId\":\"" + id + "\"}"));
            return;
        }

        Map<String,Object> params = new HashMap<>();
        params.put("id", id);
        if (! CommonHandler.checkAuthorizationResult(
                AuthorizationService.checkPrivilege(currentUser, HttpMethod.DELETE, thisCategory, "id", params, ctx),
                resp)) return;

        UserService.removeUser(id, ctx);
        resp.setBody(new DataObject("{}"));
    }

    private static void _login(JSONObject currentUser, HttpRequest req, HttpResponse resp, Context ctx) throws Exception {
        JSONObject body = CommonHandler.parseDataToJSONObject(req.getBody(), resp, "BAD_REQ_BODY", ctx);
        if (body == null) return;

        if (! CommonHandler.checkExistence(body, new String[]{"user","password"}, resp)) return;

        String id = body.getString("user");
        String password = body.getString("password");

        String token = UserService.login(id, password, ctx);

        if (token == null)
            resp.setBody(ErrorResponse.compose("BAD_LOGIN"));
        else
            resp.setBody(new DataObject("{\"result\":\""+token+"\"}"));
    }

    private static void _logout(JSONObject currentUser, HttpRequest req, HttpResponse resp, Context ctx) throws Exception {
        String id = req.getParameter("userId");
        String token = req.getHeader(UserService.HEADER_TOKEN);
        if (id!=null && id.length()==0) id = null;
        if (token!=null && token.length()==0) token = null;

        if (id != null) {
            Map<String,Object> params = new HashMap<>();
            params.put("id", id);
            if (! CommonHandler.checkAuthorizationResult(
                    AuthorizationService.checkPrivilege(currentUser, HttpMethod.DELETE, thisCategory, "login", params, ctx),
                    resp)) return;

            UserService.logout(id, null, ctx);
        }
        else if (token != null) UserService.logout(null, token, ctx);

        resp.setBody(new DataObject("{}"));
    }

    private static JSONObject _getUserFromDB(String id, HttpResponse resp, Context ctx) throws Exception {
        DataObject userDB = UserService.fetchUser(id, null, ctx);

        if (userDB == null) {
            resp.setBody(ErrorResponse.compose("NOT_FOUND", "{\"userId\":\""+id+"\"}"));
            return null;
        }

        return CommonHandler.parseDataToJSONObject(userDB, resp, "BAD_ENTITY", "userId", id, ctx);
    }

}
