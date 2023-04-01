package cloud.phusion.express.service;

import cloud.phusion.Context;
import cloud.phusion.DataObject;
import cloud.phusion.Engine;
import cloud.phusion.ExecStatus;
import cloud.phusion.express.ExpressService;
import cloud.phusion.express.component.storage.KVStorageImpl;
import cloud.phusion.express.util.CommonCode;
import cloud.phusion.integration.Integration;
import cloud.phusion.storage.DBStorage;
import cloud.phusion.storage.FileStorage;
import cloud.phusion.storage.KVStorage;
import cloud.phusion.storage.Record;
import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONArray;
import com.alibaba.fastjson2.JSONObject;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class UserService {
    private static final String USER_TABLE = "user";
    private static final String PERMISSION_TABLE = "user_permission";
    private static final String ROLES_FILE = "/roles.json";
    private static final String SESSION_USER_KEY = "SESSION_USER_";
    private static final String SESSION_KEY = "SESSION_";

    public static final String SECRET_KEY = "user.secretKey";
    public static final String SESSION_INTERVAL = "user.session.intervalInMinutes";
    public static final String HEADER_TOKEN = "x-phusion-token";

    public static final String ACTION_REFRESH_ROLES = "refreshRoles";

    private static String secret = null;
    private static long sessionIntervalInMS = 30*60*1000; // Half an hour

    public static void init(String secretKey, String interval, boolean prepareTables, Context ctx) throws Exception {
        secret = secretKey;
        if (secret!=null && secret.length()==0) secret = "";

        sessionIntervalInMS = Long.parseLong(interval) * 60 * 1000;

        if (prepareTables) prepareDBTables(ctx);
        _initRoles(ctx);

        _cacheRoles(ctx);
    }

    private static void _initRoles(Context ctx) throws Exception {
        FileStorage fStorage = ctx.getEngine().getFileStorageForApplication(ExpressService.STORAGE_ID);

        if (! fStorage.doesFileExist(ROLES_FILE, ctx)) {
            String strRoles = "[" +
                    "   {\"id\":\"admin\", \"title\":\"admin\", \"grant\":[\"* * *\"]}, " +
                    "   {\"id\":\"visitor\", \"title\":\"visitor\", \"grant\":[\"GET * *\"]}" +
                    "]";

            fStorage.saveToFile(ROLES_FILE, strRoles.getBytes(StandardCharsets.UTF_8), ctx);
        }
    }

    public static void prepareDBTables(Context ctx) throws Exception {
        DBStorage storage = ctx.getEngine().getDBStorageForApplication(ExpressService.STORAGE_ID);

        storage.prepareTable(
                USER_TABLE,
                "{" +
                        "    \"fields\": {" +
                        "        \"id\": \"String[20]\"," +
                        "        \"password\": \"String[50]\"," +
                        "        \"name\": \"String[50]\"," +
                        "        \"phone\": \"String[20]\"," +
                        "        \"email\": \"String[50]\"," +
                        "        \"desc\": \"String[5000]\"," +
                        "        \"clientId\": \"String[20]\"," +
                        "        \"icon\": \"String[200]\"," +
                        "        \"createTime\": \"String[20]\"," +
                        "        \"updateTime\": \"String[20]\"" +
                        "    }," +
                        "    \"indexes\": [" +
                        "        {\"field\": \"id\", \"primary\":true}," +
                        "        {\"field\": \"clientId\"}," +
                        "        {\"field\": \"updateTime\"}" +
                        "    ]" +
                        "}",
                ctx
        );

        storage.prepareTable(
                PERMISSION_TABLE,
                "{" +
                        "    \"fields\": {" +
                        "        \"userId\": \"String[20]\"," +
                        "        \"role\": \"String[50]\"," +
                        "        \"type\": \"String[20]\"," +
                        "        \"scope\": \"String[50]\"" +
                        "    }," +
                        "    \"indexes\": [" +
                        "        {\"fields\": [\"type\",\"scope\",\"role\"]}," +
                        "        {\"field\": \"userId\"}" +
                        "    ]" +
                        "}",
                ctx
        );

        if (storage.queryCount(USER_TABLE,"id",null,null,ctx) == 0) {
            String strAdmin = "{\"id\":\"admin\",\"password\":\"admin\",\"name\":\"Admin\",\"permissions\":[{\"role\":\"admin\"}]}";
            saveUser("admin", new DataObject(strAdmin), true, true, true, ctx);
        }
    }

    public static DataObject listRolesFromDisk(Context ctx) throws Exception {
        FileStorage storage = ctx.getEngine().getFileStorageForApplication(ExpressService.STORAGE_ID);

        if (storage.doesFileExist(ROLES_FILE, ctx)) {
            byte[] content = storage.readAllFromFile(ROLES_FILE, ctx);
            if (content == null || content.length == 0) return null;
            else return new DataObject(new String(content, StandardCharsets.UTF_8));
        }
        else return null;
    }

    private static void _cacheRoles(Context ctx) throws Exception {
        DataObject roles = listRolesFromDisk(ctx);
        if (roles == null) {
            AuthorizationService.cacheRoles(null);
            return;
        }

        JSONArray roleArr = roles.getJSONArray();
        if (roleArr==null || roleArr.size()==0) {
            AuthorizationService.cacheRoles(null);
            return;
        }

        Map<String,Object> cachedRoles = CommonCode.convertJSONArrayToMap(roleArr, "id");
        AuthorizationService.cacheRoles(cachedRoles);
    }

    public static void saveRoles(DataObject roles, Context ctx) throws Exception {
        String strRoles = roles.getString();
        if (strRoles==null || strRoles.length()==0) return;

        FileStorage storage = ctx.getEngine().getFileStorageForApplication(ExpressService.STORAGE_ID);
        storage.saveToFile(ROLES_FILE, strRoles.getBytes(StandardCharsets.UTF_8), ctx);

        ClusterService.sendMessage(ClusterService.OBJECT_USER, ACTION_REFRESH_ROLES, "*", ctx);
    }

    public static void regulateRolePrivileges(JSONArray privileges) {
        if (privileges==null || privileges.size()==0) return;

        for (int i = 0; i < privileges.size(); i++) {
            String privilege = privileges.getString(i);

            if (privilege==null || privilege.length()==0) {
                privileges.remove(i);
                i--;
            }
            else {
                String[] result = privilege.trim().split("\\s+");

                if (result.length == 1) {
                    privileges.remove(i);
                    i--;
                }
                else
                    privileges.set(i, result[0]+" "+result[1]+(result.length==3 ? " "+result[2] : ""));
            }
        }
    }

    public static void removeRole(String id, Context ctx) throws Exception {
        DataObject rolesObj = listRolesFromDisk(ctx);
        if (rolesObj == null) return;

        JSONArray roles = rolesObj.getJSONArray();
        if (roles==null || roles.size()==0) return;

        JSONObject role = CommonCode.findItemInJSONArray(roles, "id", id);
        if (role == null) return;

        roles.remove(role);
        String strRoles = roles.toJSONString();

        FileStorage storage = ctx.getEngine().getFileStorageForApplication(ExpressService.STORAGE_ID);
        storage.saveToFile(ROLES_FILE, strRoles.getBytes(StandardCharsets.UTF_8), ctx);
    }

    public static boolean hasUser(String id, Context ctx) throws Exception {
        DBStorage storage = ctx.getEngine().getDBStorageForApplication(ExpressService.STORAGE_ID);

        Record record = storage.queryRecordById(USER_TABLE, "id", "id", id, ctx);
        return record != null;
    }

    public static void saveUser(String id, DataObject user, boolean newPassword, boolean clientChanged, boolean permissionChanged, Context ctx) throws Exception {
        Engine engine = ctx.getEngine();
        JSONObject userObj = user==null ? null : user.getJSONObject();

        if (newPassword) {
            String pwd = userObj.getString("password");
            userObj.put("password", encodePassword(id, pwd));
        }

        String clientId = userObj.getString("clientId");
        if (clientChanged && clientId!=null && clientId.length()>0) {
            // Inject client scope into permissions

            JSONArray permissions = userObj.getJSONArray("permissions");
            if (permissions!=null && permissions.size()>0) {
                for (int i = 0; i < permissions.size(); i++) {
                    JSONObject permission = permissions.getJSONObject(i);
                    String scopeType = permission.getString("type");
                    if (! "integration".equals(scopeType)) { // The integration scope should not be changed
                        permission.put("type", "client");
                        permission.put("scope", JSON.parseArray("[\""+clientId+"\"]"));
                    }
                }
            }

            permissionChanged = true;
        }

        // Save to DB table "user"

        String updateTime = CommonCode.convertDatetimeString();
        String createTime = userObj.getString("createTime");
        if (createTime==null || createTime.length()==0) createTime = updateTime;
        userObj.put("createTime", createTime);
        userObj.put("updateTime", updateTime);

        String[] fields = new String[]{"id","name","password","phone","email","desc","clientId","icon","createTime","updateTime"};
        Record record = CommonCode.convertJSONObjectToRecord(userObj, fields);
        if (record == null) return;

        DBStorage storage = engine.getDBStorageForApplication(ExpressService.STORAGE_ID);
        storage.upsertRecordById(USER_TABLE, "id", record, ctx);

        // Save to DB table "user_permission"

        if (permissionChanged) {
            JSONArray permissions = userObj.getJSONArray("permissions");

            // Clear empty properties, or it has to be checked over and over.
            if (permissions!=null && permissions.size()>0) {
                for (int i = 0; i < permissions.size(); i++) {
                    JSONObject permission = permissions.getJSONObject(i);

                    String scopeType = permission.getString("type");
                    if (scopeType!=null && scopeType.trim().length()==0) {
                        permission.remove("type");
                        scopeType = null;
                    }
                    if (scopeType == null) permission.remove("scope");

                    JSONArray scopeIds = permission.getJSONArray("scope");
                    if (scopeIds!=null && scopeIds.size()==0) {
                        permission.remove("type");
                        permission.remove("scope");
                    }
                }
            }

            _savePermissions(id, new DataObject(permissions), storage, ctx);
        }
    }

    private static void _removePermissions(String id, DBStorage storage, Context ctx) throws Exception {
        String where = "userId=?";
        List<Object> params = new ArrayList<>();
        params.add(id);

        storage.deleteRecords(PERMISSION_TABLE, where, params, ctx);
    }

    private static void _savePermissions(String id, DataObject permissions, DBStorage storage, Context ctx) throws Exception {
        // Not in DB transaction, to be optimized.

        _removePermissions(id, storage, ctx); // Remove the previous permissions

        if (permissions == null) return;

        JSONArray arr = permissions.getJSONArray();
        if (arr.size() == 0) return;

        List<Object> params = new ArrayList<>();

        for (int i = 0; i < arr.size(); i++) {
            JSONObject permission = arr.getJSONObject(i);
            String role = permission.getString("role");
            String type = permission.getString("type");
            JSONArray scopes = permission.getJSONArray("scope");

            if (type==null || type.length()==0 || scopes==null || scopes.size()==0) {
                params.add(id);
                params.add(role);
                params.add(null);
                params.add(null);
                continue;
            }

            for (int j = 0; j < scopes.size(); j++) {
                params.add(id);
                params.add(role);
                params.add(type);
                params.add(scopes.getString(j));
            }
        }

        storage.insertRecords(PERMISSION_TABLE, "userId,role,type,scope", params, ctx);
    }

    public static DataObject queryUsers(Map<String,Object> query, String fields, long from, long length, Context ctx) throws Exception {
        List<Object> params = new ArrayList<>();
        String fromWhereClause = _composeFromWhereClause(query, fields, params);

        return _queryUsers(fromWhereClause, params, fields, from, length, ctx);
    }

    public static DataObject fetchUser(String id, String fields, Context ctx) throws Exception {
        DataObject usersObj = listUsersById(id, fields, ctx);
        if (usersObj == null) return null;

        JSONArray users = usersObj.getJSONArray();
        if (users==null || users.size()==0) return null;

        return new DataObject(users.getJSONObject(0));
    }

    public static DataObject listUsersById(String ids, String fields, Context ctx) throws Exception {
        if (ids==null || ids.length()==0) return null;

        StringBuilder fromWhereClause = new StringBuilder();
        fromWhereClause.append("FROM ");
        if (fields==null || fields.length()==0 || fields.contains("permissions"))
            fromWhereClause.append(ExpressService.STORAGE_ID).append('_').append(USER_TABLE)
                    .append(" U LEFT JOIN ")
                    .append(ExpressService.STORAGE_ID).append('_').append(PERMISSION_TABLE)
                    .append(" P ON P.userId=U.id");
        else
            fromWhereClause.append(ExpressService.STORAGE_ID).append('_').append(USER_TABLE);
        fromWhereClause.append(" WHERE ");

        List<Object> params = new ArrayList<>();
        CommonCode.composeWhereClauseFromStrings("id",ids,fromWhereClause,params);

        return _queryUsers(fromWhereClause.toString(), params, fields, 0, 1000, ctx);
    }

    public static long countUsers(Map<String,Object> query, Context ctx) throws Exception {
        List<Object> params = new ArrayList<>();
        String fromWhereClause = _composeFromWhereClause(query, "id", params);

        Object roleId = query==null ? null : query.get("roleId");
        Object scopeType = query==null ? null : query.get("scopeType");
        Object scope = query==null ? null : query.get("scope");

        String sql = (roleId==null && scopeType==null && scope==null) ?
                "SELECT count(1) AS c " : "SELECT count(DISTINCT id) AS c ";
        sql += fromWhereClause;

        DBStorage storage = ctx.getEngine().getDBStorageForApplication(ExpressService.STORAGE_ID);
        Record[] records = storage.freeQuery(sql, params, 0, 1, ctx);

        if (records==null || records.length==0) return 0l;
        else return records[0].getLong("c");
    }

    private static DataObject _queryUsers(String fromWhereClause, List<Object> params, String fields, long from, long length, Context ctx) throws Exception {
        boolean includePerms = true;
        Set<String> fieldSet = CommonCode.convertStringToSet(fields, "id", "id,name,desc,clientId,phone,email,permissions,icon,createTime,updateTime");

        if (fieldSet.contains("permissions")) fieldSet.remove("permissions");
        else includePerms = false;

        DBStorage storage = ctx.getEngine().getDBStorageForApplication(ExpressService.STORAGE_ID);
        JSONArray result = new JSONArray();
        Map<String,JSONObject> users = new HashMap<>(); // userId -> JSONObject
        Map<String,JSONObject> perms = new HashMap<>(); // <userId>_<role>_<type> -> JSONObject

        String selectClause = CommonCode.convertFieldSetToSelectClause(fieldSet);
        if (includePerms) selectClause += ",P.role AS role,P.type AS type,P.scope AS scope";
        String orderClause = fromWhereClause.length()>0 ? " " : "";
        orderClause += includePerms ? "ORDER BY U.updateTime desc" : "ORDER BY updateTime desc";

        String sql = "SELECT "+selectClause+" "+fromWhereClause+orderClause;

        Record[] list = storage.freeQuery(sql, params, from, length, ctx);

        if (list!=null && list.length>0) {
            for (Record record : list) {
                String id = record.getString("id");
                JSONObject item;

                if (! includePerms || includePerms && ! users.containsKey(id)) {
                    item = new JSONObject();
                    CommonCode.copyRecordFields(record, item, fieldSet);
                    result.add(item);
                    if (includePerms) users.put(id, item);
                }
                else
                    item = users.get(id);

                if (includePerms) {
                    String role = record.getString("role");
                    String type = record.getString("type");
                    String scope = record.getString("scope");

                    if (role!=null && role.length()>0) {
                        if (type!=null && type.length()==0) type = null;
                        if (type==null || scope!=null && scope.length()==0) scope = null;

                        String index = id+"_"+role;
                        if (type != null) index += "_"+type;

                        JSONObject perm = perms.get(index);
                        if (perm == null) {
                            perm = new JSONObject();
                            perm.put("role",role);
                            if (type != null) perm.put("type",type);

                            perms.put(index, perm);

                            JSONArray userPerms = item.getJSONArray("permissions");
                            if (userPerms == null) {
                                userPerms = new JSONArray();
                                item.put("permissions", userPerms);
                            }
                            userPerms.add(perm);
                        }

                        if (scope != null) {
                            JSONArray scopes = perm.getJSONArray("scope");
                            if (scopes == null) {
                                scopes = new JSONArray();
                                perm.put("scope", scopes);
                            }

                            scopes.add(scope);
                        }
                    }
                }
            }
        }

        return new DataObject(result);
    }

    private static String _composeFromWhereClause(Map<String,Object> query, String fields, List<Object> params) {
        String search = query==null ? null : (String) query.get("search");
        String phone = query==null ? null : (String) query.get("phone");
        String email = query==null ? null : (String) query.get("email");
        String clientId = query==null ? null : (String) query.get("clientId");
        String roleId = query==null ? null : (String) query.get("roleId");
        String scopeType = query==null ? null : (String) query.get("scopeType");
        String scope = query==null ? null : (String) query.get("scope");

        StringBuilder sql = new StringBuilder();
        boolean includePerms = (fields==null || fields.length()==0 || fields.contains("permissions"));

        sql.append("FROM ").append(ExpressService.STORAGE_ID).append('_').append(USER_TABLE);

        if (roleId!=null || scopeType!=null || scope!=null || includePerms) {
            sql.append(" U");
            if (includePerms) sql.append(" LEFT");
            sql.append(" JOIN ")
                    .append(ExpressService.STORAGE_ID).append('_').append(PERMISSION_TABLE)
                    .append(" P ON U.id=P.userId");
        }

        if (search!=null || phone!=null || email!=null || clientId!=null || roleId!=null || scopeType!=null || scope!=null)
            sql.append(" WHERE ");

        boolean[] hasWhere = new boolean[]{false};
        CommonCode.composeWhereClause(hasWhere, "name", search, "like", sql, params);
        CommonCode.composeWhereClause(hasWhere, "phone", phone, sql, params);
        CommonCode.composeWhereClause(hasWhere, "email", email, sql, params);
        CommonCode.composeWhereClause(hasWhere, "clientId", clientId, sql, params);
        CommonCode.composeWhereClause(hasWhere, "P.role", roleId, sql, params);
        CommonCode.composeWhereClause(hasWhere, "P.type", scopeType, sql, params);
        CommonCode.composeWhereClause(hasWhere, "P.scope", scope, sql, params);

        return sql.length()==0 ? null : sql.toString();
    }

    public static void removeUser(String id, Context ctx) throws Exception {
        DBStorage storage = ctx.getEngine().getDBStorageForApplication(ExpressService.STORAGE_ID);
        storage.deleteRecordById(USER_TABLE, "id", id, ctx);

        _removePermissions(id, storage, ctx);
    }

    public static String encodePassword(String id, String password) throws Exception {
        return _digest(id+secret+password);
    }

    private static String _digest(String str) throws Exception {
        MessageDigest md = MessageDigest.getInstance("MD5");

        byte[] hash = md.digest(str.getBytes());

        StringBuilder result = new StringBuilder();
        for (byte b : hash) {
            result.append(String.format("%02x", b));
        }

        return result.toString();
    }

    private static String _generateToken(Context ctx) throws Exception {
        String part1 = ""+ctx.getEngine().generateUniqueId(ctx);

        Random r = new Random();
        String part2 = ""+r.nextDouble();

        return _digest(part1).toUpperCase() + _digest(part2).toUpperCase();
    }

    /**
     * Return the token. Null, if the login is failed.
     */
    public static String login(String id, String password, Context ctx) throws Exception {
        if (password==null || password.length()==0) return null;

        DataObject userObj = fetchUser(id, "id,password,name,clientId,permissions", ctx);
        if (userObj == null) return null;

        JSONObject user = userObj.getJSONObject();
        if (user == null) return null;

        // Check password
        String passwordDB = user.getString("password");
        if (! encodePassword(id,password).equals(passwordDB) ) return null;

        KVStorage storage = ctx.getEngine().getKVStorageForApplication(ExpressService.STORAGE_ID);

        // If the user has already logged in, remove the previous session

        String prevToken = (String) storage.get(SESSION_USER_KEY+id, ctx);
        if (prevToken != null) {
            storage.remove(SESSION_KEY+prevToken, ctx);
        }

        // Save the token and user info as a new session

        String token = _generateToken(ctx);

        user.put("time", System.currentTimeMillis());
        String userInfo = user.toJSONString();

        storage.put(SESSION_USER_KEY+id, token, sessionIntervalInMS);
        storage.put(SESSION_KEY+token, userInfo, sessionIntervalInMS);

        return token;
    }

    /**
     * Return {id, name, clientId, permissions, time}. Null, if not logged in.
     */
    public static JSONObject getCurrentUser(String token, Context ctx) throws Exception {
        if (token==null || token.length()==0) return null;

        KVStorage storage = ctx.getEngine().getKVStorageForApplication(ExpressService.STORAGE_ID);
        String userInfo = (String) storage.get(SESSION_KEY+token);
        if (userInfo == null) return null;

        JSONObject user = JSON.parseObject(userInfo);

        // Refresh the session time, if there's 40% of valid interval left.

        long sessionTime = user.getLongValue("time", 0);
        long currentTime = System.currentTimeMillis();
        if (((double)(currentTime-sessionTime)) / ((double)sessionIntervalInMS) > 0.6) {
            user.put("time", currentTime);
            userInfo = user.toJSONString();

            storage.put(SESSION_USER_KEY+user.getString("id"), token, sessionIntervalInMS);
            storage.put(SESSION_KEY+token, userInfo, sessionIntervalInMS);
        }

        return user;
    }

    public static void logout(String id, String token, Context ctx) throws Exception {
        KVStorage storage = ctx.getEngine().getKVStorageForApplication(ExpressService.STORAGE_ID);

        if (id != null) {
            String prevToken = (String) storage.get(SESSION_USER_KEY + id, ctx);
            if (prevToken != null) {
                storage.remove(SESSION_USER_KEY + id, ctx);
                storage.remove(SESSION_KEY + prevToken, ctx);
            }
        }

        if (token != null)
            storage.remove(SESSION_KEY + token, ctx);
    }

    public static void performAction(String action, String id, Context ctx) throws Exception {
        switch (action) {
            case ACTION_REFRESH_ROLES: _cacheRoles(ctx); break;
        }
    }

}
