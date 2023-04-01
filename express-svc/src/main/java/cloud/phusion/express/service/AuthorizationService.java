package cloud.phusion.express.service;

import cloud.phusion.Context;
import cloud.phusion.DataObject;
import cloud.phusion.express.util.CommonCode;
import cloud.phusion.protocol.http.HttpMethod;
import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONArray;
import com.alibaba.fastjson2.JSONObject;

import java.util.*;

public class AuthorizationService {
    private static final String ERR_NO_OP = "Operation not supported";
    private static Map<String,Object> cachedRoles = new HashMap<>();

    public static void cacheRoles(Map<String,Object> roles) {
        if (roles != null) cachedRoles = roles;
    }

    public static Map<String,Object> getCachedRoles() {
        return cachedRoles;
    }

    /**
     * params: for "***Ids" parameter, the value type should be "Set<String>"
     * Return: null if passed; or, [error_code, error_reason]
     */
    public static String[] checkPrivilege(JSONObject currentUser, HttpMethod method, String category, String item, Map<String,Object> params, Context ctx) throws Exception {
        if (currentUser == null) return new String[]{"NOT_LOGIN"};

        StringBuilder errReason = new StringBuilder();

        switch (category) {
            case "user": _checkUserPrivilege(currentUser, method, item, params, errReason, ctx); break;
            case "transaction": _checkTransactionPrivilege(currentUser, method, item, params, errReason, ctx); break;
            case "module": _checkModulePrivilege(currentUser, method, item, params, errReason, ctx); break;
            case "cluster": _checkClusterPrivilege(currentUser, method, item, params, errReason, ctx); break;
            case "client": _checkClientPrivilege(currentUser, method, item, params, errReason, ctx); break;
            case "connection": _checkConnectionPrivilege(currentUser, method, item, params, errReason, ctx); break;
            case "application": _checkApplicationPrivilege(currentUser, method, item, params, errReason, ctx); break;
            case "integration": _checkIntegrationPrivilege(currentUser, method, item, params, errReason, ctx); break;
            default: errReason.append(ERR_NO_OP);
        }

        if (errReason.length() == 0) return null;
        else return new String[]{"NO_PRIV", errReason.toString()};
    }

    private static void _checkUserPrivilege(JSONObject currentUser, HttpMethod method, String item, Map<String,Object> params, StringBuilder errReason, Context ctx) throws Exception {
        String category = "user";

        String currentUserId = currentUser.getString("id");
        String userId = params==null ? null : (String) params.get("id");
        String clientId = params==null ? null : (String) params.get("clientId");
        if (clientId!=null && clientId.length()==0) clientId = null;

        String scopeType = clientId==null ? null : "client";

        switch (method) {
            case PUT:
                if (item == null) { // Save user
                    if (currentUserId.equals(userId)) {
                        // A user can update his/her own information except permissions
                        if (params.get("permissions") != null) errReason.append("Can not update your own permissions");
                    }
                    else {
                        _checkGeneralPrivilege(currentUser, method, category, item, scopeType, clientId, errReason, ctx);

                        String currentClientId = currentUser.getString("clientId");
                        JSONArray permissions = params==null ? null : (JSONArray) params.get("permissions");
                        if (currentClientId!=null && permissions!=null && permissions.size()>0) {
                            // Check whether the client admin assigned privileges beyond his/her authority
                            _checkPermissionsAssignedByClient(currentClientId, permissions, errReason, ctx);
                        }
                    }
                }
                else if (item.equals("role")) { // Save role
                    _checkGeneralPrivilege(currentUser, method, category, item, null, null, errReason, ctx);
                }
                else errReason.append(ERR_NO_OP);
                break;
            case GET:
                if (item==null || item.equals("id")) { // Count/list users, Get user
                    // Any user in the same scope can do this.

                    String currentClientId = currentUser.getString("clientId");
                    if (currentClientId!=null && currentClientId.length()==0) currentClientId = null;

                    if ((clientId==null && currentClientId!=null ||
                            clientId!=null && currentClientId!=null && !clientId.equals(currentClientId)) &&
                            !currentUserId.equals(userId))
                        errReason.append("Can not get user information beyond your authority");
                }
                else if (item.equals("role")) {} // List roles. Anyone can do this.
                else errReason.append(ERR_NO_OP);
                break;
            case DELETE:
                if (userId!=null && clientId==null) {
                    clientId = _getUserClient(userId, ctx);
                    if (clientId!=null && clientId.length()==0) clientId = null;
                    scopeType = clientId==null ? null : "client";
                }

                if (item == null) errReason.append(ERR_NO_OP);
                else if (item.equals("login") || item.equals("id")) { // Logout user (not myself), Remove user
                    _checkGeneralPrivilege(currentUser, method, category, item, scopeType, clientId, errReason, ctx);
                }
                else if (item.equals("role")) { // Remove role
                    _checkGeneralPrivilege(currentUser, method, category, item, null, null, errReason, ctx);
                }
                else errReason.append(ERR_NO_OP);
                break;
            default: errReason.append(ERR_NO_OP);
        }
    }

    private static void _checkTransactionPrivilege(JSONObject currentUser, HttpMethod method, String item, Map<String,Object> params, StringBuilder errReason, Context ctx) throws Exception {
        String category = "transaction";

        String integrationId = params==null ? null : (String) params.get("integrationId");
        String clientId = params==null ? null : (String) params.get("clientId");
        String applicationId = params==null ? null : (String) params.get("applicationId");

        if (integrationId!=null && integrationId.length()==0) integrationId = null;
        if (clientId!=null && clientId.length()==0) clientId = null;
        if (applicationId!=null && applicationId.length()==0) applicationId = null;

        String scopeType = null;
        String scopeId = null;
        if (integrationId != null) {
            scopeType = "integration";
            scopeId = integrationId;
        }
        if (clientId != null) {
            scopeType = "client";
            scopeId = clientId;
        }
        if (applicationId != null) {
            scopeType = "application";
            scopeId = applicationId;
        }

        // List transactions, Get transaction stats, Get transaction group stats, Get transaction, Get transaction step stats
        _checkGeneralPrivilege(currentUser, method, category, item, scopeType, scopeId, errReason, ctx);
    }

    private static void _checkModulePrivilege(JSONObject currentUser, HttpMethod method, String item, Map<String,Object> params, StringBuilder errReason, Context ctx) throws Exception {
        String category = "module";

        String currentUserId = currentUser.getString("id");
        String userId = params==null ? null : (String) params.get("owner");
        if (userId!=null && userId.length()==0) userId = null;
        String filename = params==null ? null : (String) params.get("filename");
        if (filename!=null && filename.length()==0) filename = null;

        String itId = null;
        if (ModuleService.CODE_OWNER_INTEGRATION.equals(userId)) {
            userId = null;
            if (filename != null) {
                int pos = filename.lastIndexOf("." + CodeRunner.CODE_TYPE_NODE);
                itId = pos>0 ? filename.substring(0, pos) : null;
            }
        }

        String scopeType = null;

        switch (method) {
            case PUT:
                if (item!=null && item.equals("java")) { // Save java module
                    _checkGeneralPrivilege(currentUser, method, category, item, null, null, errReason, ctx);
                }
                else errReason.append(ERR_NO_OP);
                break;
            case GET:
                if (item == null) errReason.append(ERR_NO_OP);
                else if (item.equals("java") || item.equals("nodejs")) { // Get java module, List nodejs modules
                    _checkGeneralPrivilege(currentUser, method, category, item, null, null, errReason, ctx);
                }
                else if (item.equals("id") || item.equals("encode")) { // Get id, Encode
                    // Anyone can do this.
                }
                else if (item.equals("code")) { // Get code
                    if (userId!=null && !userId.equals("all") && !currentUserId.equals(userId)) {
                        errReason.append("Can not manipulate others' code");
                        return;
                    }

                    if (itId !=  null) scopeType = "integration";
                    if ("all".equals(userId)) item = "listallcode";
                    _checkGeneralPrivilege(currentUser, method, category, item, scopeType, itId, errReason, ctx);
                }
                else errReason.append(ERR_NO_OP);
                break;
            case POST:
            case DELETE:
                if (item == null) errReason.append(ERR_NO_OP);
                else if (item.equals("java") || item.equals("nodejs")) { // Save jar, Install nodejs, Remove jar, Uninstall nodejs
                    _checkGeneralPrivilege(currentUser, method, category, item, null, null, errReason, ctx);
                }
                else if (item.equals("code")) { // Save/run code, Remove code
                    if (userId!=null && !currentUserId.equals(userId)) {
                        errReason.append("Can not manipulate others' code");
                        return;
                    }

                    if (itId !=  null) scopeType = "integration";
                    _checkGeneralPrivilege(currentUser, method, category, item, scopeType, itId, errReason, ctx);
                }
                else errReason.append(ERR_NO_OP);
                break;
            default: errReason.append(ERR_NO_OP);
        }
    }

    private static void _checkClusterPrivilege(JSONObject currentUser, HttpMethod method, String item, Map<String,Object> params, StringBuilder errReason, Context ctx) throws Exception {
        String category = "cluster";

        // List engines, List objects
        _checkGeneralPrivilege(currentUser, method, category, item, null, null, errReason, ctx);
    }

    private static void _checkClientPrivilege(JSONObject currentUser, HttpMethod method, String item, Map<String,Object> params, StringBuilder errReason, Context ctx) throws Exception {
        String category = "client";

        String clientId = params==null ? null : (String) params.get("id");
        if (clientId==null || clientId.length()==0) clientId = params==null ? null : (String) params.get("clientId");
        if (clientId!=null && clientId.length()==0) clientId = null;

        String scopeType = clientId==null ? null : "client";

        switch (method) {
            case PUT:
                if (item == null) { // Save client
                    _checkGeneralPrivilege(currentUser, method, category, item, scopeType, clientId, errReason, ctx);
                }
                else if (item.equals("table")) { // Save table
                    _checkGeneralPrivilege(currentUser, method, category, item, scopeType, clientId, errReason, ctx);
                }
                else errReason.append(ERR_NO_OP);
                break;
            case GET:
                if (item == null) { // List clients
                    Set<String> ids = params==null ? null : (Set<String>) params.get("clientIds");
                    if (ids!=null && ids.size()>0)
                        _checkGeneralPrivilege(currentUser, method, category, item, "client", ids, errReason, ctx);
                    else
                        _checkGeneralPrivilege(currentUser, method, category, item, null, null, errReason, ctx);
                }
                else if (item.equals("category")) { // List categories
                    _checkGeneralPrivilege(currentUser, method, category, item, null, null, errReason, ctx);
                }
                else if (item.equals("id") || item.equals("table") || item.equals("connection")) { // Get client, Get table, List connections
                    _checkGeneralPrivilege(currentUser, method, category, item, scopeType, clientId, errReason, ctx);
                }
                else errReason.append(ERR_NO_OP);
                break;
            case DELETE:
                if (item == null) errReason.append(ERR_NO_OP);
                else if (item.equals("table")) { // Remove table
                    _checkGeneralPrivilege(currentUser, method, category, item, scopeType, clientId, errReason, ctx);
                }
                else if (item.equals("id")) { // Remove client
                    _checkGeneralPrivilege(currentUser, method, category, item, null, null, errReason, ctx);
                }
                else errReason.append(ERR_NO_OP);
                break;
            default: errReason.append(ERR_NO_OP);
        }
    }

    private static void _checkConnectionPrivilege(JSONObject currentUser, HttpMethod method, String item, Map<String,Object> params, StringBuilder errReason, Context ctx) throws Exception {
        String category = "connection";

        String connId = params==null ? null : (String) params.get("id");
        if (connId!=null && connId.length()==0) connId = null;
        String clientId = params==null ? null : (String) params.get("clientId");
        if (clientId!=null && clientId.length()==0) clientId = null;

        String scopeType = clientId==null ? null : "client";

        switch (method) {
            case PUT:
                if (item == null) { // Save connection
                    _checkGeneralPrivilege(currentUser, method, category, item, scopeType, clientId, errReason, ctx);
                }
                else errReason.append(ERR_NO_OP);
                break;
            case GET:
                if (item == null) { // Count/list connections
                    _checkGeneralPrivilege(currentUser, method, category, item, scopeType, clientId, errReason, ctx);
                    if (errReason.length() > 0) errReason.append(". If you are a client member, try to include 'clientId' in the 'fields'");
                }
                else if (item.equals("id")) { // Get connection
                    _checkGeneralPrivilege(currentUser, method, category, item, scopeType, clientId, errReason, ctx);
                }
                else errReason.append(ERR_NO_OP);
                break;
            case POST:
            case DELETE:
                if (item!=null &&
                        (method.equals(HttpMethod.POST) && item.equals("operation") ||
                                method.equals(HttpMethod.DELETE) && item.equals("id"))) { // Remove connection, Operate connection
                    if (connId != null) {
                        clientId = _getConnectionClient(connId, ctx);
                        if (clientId!=null && clientId.length()==0) clientId = null;
                        scopeType = clientId==null ? null : "client";
                    }
                    _checkGeneralPrivilege(currentUser, method, category, item, scopeType, clientId, errReason, ctx);
                }
                else errReason.append(ERR_NO_OP);
                break;
            default: errReason.append(ERR_NO_OP);
        }
    }

    private static void _checkApplicationPrivilege(JSONObject currentUser, HttpMethod method, String item, Map<String,Object> params, StringBuilder errReason, Context ctx) throws Exception {
        String category = "application";

        String appId = params==null ? null : (String) params.get("id");
        if (appId==null || appId.length()==0) appId = params==null ? null : (String) params.get("applicationId");
        if (appId!=null && appId.length()==0) appId = null;

        String scopeType = appId==null ? null : "application";

        switch (method) {
            case PUT:
                if (item == null) { // Save application
                    _checkGeneralPrivilege(currentUser, method, category, item, scopeType, appId, errReason, ctx);
                }
                else if (item.equals("endpoint") || item.equals("table")) { // Save endpoint, Save table
                    _checkGeneralPrivilege(currentUser, method, category, item, scopeType, appId, errReason, ctx);
                }
                else errReason.append(ERR_NO_OP);
                break;
            case GET:
                if (item == null) { // List applications
                    // Anyone can do this.
                }
                else if (item.equals("category") || item.equals("protocol")) { // List categories, List protocols
                    // Anyone can do this.
                }
                else if (item.equals("endpoint") || item.equals("table") || item.equals("id") || item.equals("connection")) { // List connection, Get application, Get table, Get endpoint
                    _checkGeneralPrivilege(currentUser, method, category, item, scopeType, appId, errReason, ctx);
                }
                else errReason.append(ERR_NO_OP);
                break;
            case POST:
                if (item!=null && item.equals("operation")) { // Operate application
                    _checkGeneralPrivilege(currentUser, method, category, item, scopeType, appId, errReason, ctx);
                }
                else errReason.append(ERR_NO_OP);
                break;
            case DELETE:
                if (item == null) errReason.append(ERR_NO_OP);
                else if (item.equals("id") || item.equals("endpoint") || item.equals("table")) { // Remove application, Remove endpoint, Remove table
                    _checkGeneralPrivilege(currentUser, method, category, item, scopeType, appId, errReason, ctx);
                }
                else errReason.append(ERR_NO_OP);
                break;
            default: errReason.append(ERR_NO_OP);
        }
    }

    private static void _checkIntegrationPrivilege(JSONObject currentUser, HttpMethod method, String item, Map<String,Object> params, StringBuilder errReason, Context ctx) throws Exception {
        String category = "integration";

        String itId = params==null ? null : (String) params.get("id");
        if (itId==null || itId.length()==0) itId = params==null ? null : (String) params.get("integrationId");
        if (itId!=null && itId.length()==0) itId = null;
        String clientId = params==null ? null : (String) params.get("clientId");
        if (clientId!=null && clientId.length()==0) clientId = null;

        String scopeType = null;

        switch (method) {
            case PUT:
                if (item==null || item.equals("table")) { // Save integration, Save table
                    if (clientId != null) {
                        _checkGeneralPrivilege(currentUser, method, category, item, "client", clientId, errReason, ctx);
                        if (errReason.length() == 0) return;
                        else errReason.setLength(0); // If "client" scope check failed, try to check "integration" scope.
                    }
                    if (itId != null) scopeType = "integration";
                    _checkGeneralPrivilege(currentUser, method, category, item, scopeType, itId, errReason, ctx);
                }
                else errReason.append(ERR_NO_OP);
                break;
            case GET:
                if (item == null) { // Count/list integrations
                    if (clientId != null) {
                        _checkGeneralPrivilege(currentUser, method, category, item, "client", clientId, errReason, ctx);
                        if (errReason.length() == 0) return;
                        else errReason.setLength(0); // If "client" scope check failed, try to check "integration" scope.
                    }

                    Set<String> ids = params==null ? null : (Set<String>) params.get("integrationIds");
                    if (ids != null) scopeType = "integration";
                    _checkGeneralPrivilege(currentUser, method, category, item, scopeType, ids, errReason, ctx);
                }
                else if (item.equals("id") || item.equals("table")) { // Get integration, Get table
                    if (clientId != null) {
                        _checkGeneralPrivilege(currentUser, method, category, item, "client", clientId, errReason, ctx);
                        if (errReason.length() == 0) return;
                        else errReason.setLength(0); // If "client" scope check failed, try to check "integration" scope.
                    }
                    if (itId != null) scopeType = "integration";
                    _checkGeneralPrivilege(currentUser, method, category, item, scopeType, itId, errReason, ctx);
                }
                else if (item.equals("category") || item.equals("template")) {} // Get categories, Get templates. Anyone can do this.
                else errReason.append(ERR_NO_OP);
                break;
            case POST:
                if (item == null) errReason.append(ERR_NO_OP);
                else if (item.equals("test") || item.equals("operation")) { // Test integration, Operate integration
                    if (itId != null) scopeType = "integration";
                    _checkGeneralPrivilege(currentUser, method, category, item, scopeType, itId, errReason, ctx);
                }
                else errReason.append(ERR_NO_OP);
                break;
            case DELETE:
                if (item == null) errReason.append(ERR_NO_OP);
                else if (item.equals("id") || item.equals("table")) { // Remove integration, Remove table
                    if (itId != null) scopeType = "integration";
                    _checkGeneralPrivilege(currentUser, method, category, item, scopeType, itId, errReason, ctx);
                }
                else errReason.append(ERR_NO_OP);
                break;
            default: errReason.append(ERR_NO_OP);
        }
    }

    private static void _checkGeneralPrivilege(JSONObject currentUser, HttpMethod method, String category, String item, String scopeType, Object scope, StringBuilder errReason, Context ctx) throws Exception {
        JSONArray permissions = currentUser.getJSONArray("permissions");
        if (permissions==null || permissions.size()==0) {
            if (errReason.length() > 0) errReason.append(". ");
            errReason.append("There's not any privilege assigned");
            return;
        }

        Map<String,String> clientIdOfIntegrations = new HashMap<>(); // integrationId -> clientId. For caching.
        if (_doPrivilegeCheck(currentUser,method,category,item,scopeType,scope,false,clientIdOfIntegrations,ctx))
            return;

        if (errReason.length() > 0) errReason.append(". ");
        errReason.append("Has no privilege to perform the action");
    }

    /**
     * ignoreScope:
     *      Assume that there's a user has limited (i.e. scoped) permission of "PUT integration id" (scope = integration 100011),
     *      and the queried permission has no scope specified (i.e. scopeType=null), then:
     *      if ignoreScope = true, this function will return true, because the user really do not have the privilege;
     *      if ignoreScope = false, this function will return false, it means the user can see the menu or button of "Edit integration"
     *      but has limited privileges.
     * scope: String, or Set<String>
     */
    private static boolean _doPrivilegeCheck(JSONObject currentUser, HttpMethod method, String category, String item, String scopeType, Object scope, boolean ignoreScope, Map<String,String> clientIdOfIntegrations, Context ctx) throws Exception {
        JSONArray permissions = currentUser.getJSONArray("permissions");
        String clientIdOfIntegration;
        String currentScopeType;
        JSONArray currentScopeIds;

        Set<String> scopeIds;
        if (scope instanceof String) {
            scopeIds = new HashSet<>();
            scopeIds.add((String) scope);
        }
        else scopeIds = (Set<String>) scope;

        for (int i = 0; i < permissions.size(); i++) {
            JSONObject permission = permissions.getJSONObject(i);
            String role = permission.getString("role");

            // Check role permissions

            boolean passed = _matchRole(role, method, category, item);

            if (ignoreScope) {
                if (passed) return true;
                else continue;
            }
            else if (! passed) continue;

            // Check the scope of the permission

            passed = false;
            currentScopeType = permission.getString("type");
            currentScopeIds = permission.getJSONArray("scope");

            if (currentScopeType == null) passed = true;
            else if (scopeType != null) {
                if (scopeType.equals(currentScopeType)) {
                    boolean notMatched = false;
                    for (String scopeId : scopeIds) {
                        if (! currentScopeIds.contains(scopeId)) {
                            notMatched = true;
                            break;
                        }
                    }
                    if (! notMatched) passed = true;
                }
                else {
                    if (scopeType.equals("integration") && currentScopeType.equals("client")) {
                        // Check whether the integration belongs to the client

                        boolean notMatched = false;
                        for (String scopeId : scopeIds) {
                            clientIdOfIntegration = clientIdOfIntegrations.get(scopeId);
                            if (clientIdOfIntegration == null) {
                                _retrieveClientOfIntegrations(scopeIds, clientIdOfIntegrations, ctx);
                                clientIdOfIntegration = clientIdOfIntegrations.get(scopeId);
                            }

                            if (! currentScopeIds.contains(clientIdOfIntegration)) {
                                notMatched = true;
                                break;
                            }
                        }
                        if (! notMatched) passed = true;
                    }
                }
            }

            if (passed) return true;
        }

        return false;
    }

    private static boolean _matchRole(String roleId, HttpMethod method, String category, String item) {
        JSONObject role = (JSONObject) cachedRoles.get(roleId);
        if (role == null) return false;

        boolean result = false;

        // Match grant list

        JSONArray arr = role.getJSONArray("grant");
        if (arr==null || arr.size()==0) return false;

        for (int i = 0; i < arr.size(); i++) {
            if (_matchRolePrivilege(arr.getString(i), method, category, item)) {
                result = true;
                break;
            }
        }

        if (! result) return false;

        // Match revoke list

        arr = role.getJSONArray("revoke");
        if (arr==null || arr.size()==0) return true;

        for (int i = 0; i < arr.size(); i++) {
            if (_matchRolePrivilege(arr.getString(i), method, category, item)) return false;
        }

        return result;
    }

    private static boolean _matchRolePrivilege(String privilege, HttpMethod method, String category, String item) {
        int pos1 = privilege.indexOf(' ');
        int pos2 = privilege.indexOf(' ', pos1+1);

        String pMethod = privilege.substring(0,pos1);
        String pCategory = pos2<0 ? privilege.substring(pos1+1) : privilege.substring(pos1+1,pos2);
        String pItem = pos2<0 ? null : privilege.substring(pos2+1);

        if (!pMethod.equals("*") && !pMethod.equalsIgnoreCase(method.toString())) return false;
        if (!pCategory.equals("*") && !pCategory.equalsIgnoreCase(category)) return false;

        if (pItem==null && item!=null || pItem!=null &&
                !pItem.equals("*") && !pItem.equalsIgnoreCase(item)) return false;

        return true;
    }

    private static void _checkPermissionsAssignedByClient(String clientId, JSONArray permissions, StringBuilder errReason, Context ctx) throws Exception {
        Set<String> integrationsOfClient = null;

        for (int i = 0; i < permissions.size(); i++) {
            JSONObject permission = permissions.getJSONObject(i);
            String scopeType = permission.getString("type");
            String[] scopeIds = permission.getJSONArray("scope").toArray(String.class);

            if ("application".equals(scopeType)) {
                if (errReason.length() > 0) errReason.append(". ");
                errReason.append("Can not assign an application permission");
                return;
            }
            else if ("client".equals(scopeType)) {
                for (String scopeId : scopeIds) {
                    if (! clientId.equals(scopeId)) {
                        if (errReason.length() > 0) errReason.append(". ");
                        errReason.append("Can not assign other clients' permissions");
                        return;
                    }
                }
            }
            else if ("integration".equals(scopeType)) {
                if (integrationsOfClient == null)
                    integrationsOfClient = _loadIntegrations("clientId", clientId, ctx);

                for (String scopeId : scopeIds) {
                    if (! integrationsOfClient.contains(scopeId)) {
                        if (errReason.length() > 0) errReason.append(". ");
                        errReason.append("Can not assign integration permissions which belongs to others");
                        return;
                    }
                }
            }
        }
    }

    private static Set<String> _loadIntegrations(String key, String value, Context ctx) throws Exception {
        Set<String> result = new HashSet<>();
        Map<String, Object> query = new HashMap<>();
        query.put(key, value);

        DataObject its = IntegrationService.queryIntegrations(query, "id", 0, 1000, ctx);
        JSONArray itArr = its==null ? null : its.getJSONArray();
        if (itArr==null || itArr.size()==0) return new HashSet<>();

        return CommonCode.convertJSONArrayToMap(itArr,"id").keySet();
    }

    private static void _retrieveClientOfIntegrations(Set<String> ids, Map<String,String> clientIdOfIntegrations, Context ctx) throws Exception {
        StringBuilder strIds = new StringBuilder();
        boolean isFirst = true;
        for (String id : ids) {
            if (isFirst) isFirst = false;
            else strIds.append(',');
            strIds.append(id);
        }

        DataObject result = IntegrationService.listIntegrationsById(strIds.toString(), "id,clientId", ctx);
        if (result == null) return;
        JSONArray itArr = result.getJSONArray();
        if (itArr==null || itArr.size()==0) return;

        for (int i = 0; i < itArr.size(); i++) {
            JSONObject item = itArr.getJSONObject(i);
            clientIdOfIntegrations.put(item.getString("id"), item.getString("clientId"));
        }
    }

    private static String _getIntegrationClient(String integrationId, Context ctx) throws Exception {
        DataObject result = IntegrationService.listIntegrationsById(integrationId, "clientId", ctx);
        if (result == null) return "";
        JSONArray arr = result.getJSONArray();
        if (arr==null || arr.size()==0) return "";
        else return arr.getJSONObject(0).getString("clientId");
    }

    private static String _getUserClient(String userId, Context ctx) throws Exception {
        DataObject result = UserService.fetchUser(userId, "clientId", ctx);
        if (result == null) return "";
        JSONObject obj = result.getJSONObject();
        if (obj == null) return "";
        else return obj.getString("clientId");
    }

    private static String _getConnectionClient(String connectionId, Context ctx) throws Exception {
        DataObject result = ConnectionService.listConnectionsById(connectionId, "clientId", ctx);
        if (result == null) return "";
        JSONArray arr = result.getJSONArray();
        if (arr==null || arr.size()==0) return "";
        else return arr.getJSONObject(0).getString("clientId");
    }

    public static DataObject checkPrivileges(JSONObject currentUser, JSONArray permissions, Context ctx) throws Exception {
        if (currentUser==null || permissions==null) return null;

        JSONArray currentPermissions = currentUser.getJSONArray("permissions");
        boolean forceToFalse = false;
        if (currentPermissions==null || currentPermissions.size()==0) forceToFalse = true;

        Map<String,String> clientIdOfIntegrations = new HashMap<>(); // integrationId -> clientId. For caching.

        for (int i = 0; i < permissions.size(); i++) {
            JSONObject permission = permissions.getJSONObject(i);
            String scopeType = permission.getString("scopeType");
            JSONObject operations = permission.getJSONObject("operations");

            if (scopeType!=null && scopeType.length()==0) scopeType = null;
            if (operations==null || operations.size()==0) continue;

            JSONArray scopeIdArr = permission.getJSONArray("scope");
            Set<String> scopeIds = null;
            if (scopeIdArr != null) {
                scopeIds = new HashSet<>();
                scopeIds.addAll(scopeIdArr.toList(String.class));
            }

            Set<String> operationKeys = operations.keySet();
            for (String operation : operationKeys) {
                if (forceToFalse)
                    operations.put(operation, false);
                else {
                    int pos1 = operation.indexOf(' ');
                    int pos2 = operation.indexOf(' ', pos1+1);

                    HttpMethod pMethod = HttpMethod.valueOf(operation.substring(0,pos1));
                    String pCategory = pos2<0 ? operation.substring(pos1+1) : operation.substring(pos1+1,pos2);
                    String pItem = pos2<0 ? null : operation.substring(pos2+1);

                    boolean result;

                    if (scopeType == null)
                        result = _doPrivilegeCheck(currentUser,pMethod,pCategory,pItem,null,null,true,clientIdOfIntegrations,ctx);
                    else
                        result = _doPrivilegeCheck(currentUser,pMethod,pCategory,pItem,scopeType,scopeIds,false,clientIdOfIntegrations,ctx);
                    operations.put(operation, result);
                }
            }
        }

        return new DataObject(permissions);
    }

}
