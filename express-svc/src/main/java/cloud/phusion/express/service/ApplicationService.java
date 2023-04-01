package cloud.phusion.express.service;

import cloud.phusion.*;
import cloud.phusion.application.Application;
import cloud.phusion.express.ExpressService;
import cloud.phusion.express.util.CommonCode;
import cloud.phusion.express.util.ServiceLogger;
import cloud.phusion.storage.DBStorage;
import cloud.phusion.storage.FileStorage;
import cloud.phusion.storage.Record;
import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONArray;
import com.alibaba.fastjson2.JSONObject;

import java.nio.charset.StandardCharsets;
import java.util.*;

public class ApplicationService {
    private static final String _position = ApplicationService.class.getName();

    private static final String ROOT_PATH = "/application/";
    private static final String APP_TABLE = "application";

    public static final String STATUS_NONE = "unused";
    public static final String STATUS_STOP = "stopped";
    public static final String STATUS_RUN = "running";

    public static final String ACTION_START = "start";
    public static final String ACTION_STOP = "stop";
    public static final String ACTION_RESTART = "restart";

    public static void init(boolean prepareTables, Context ctx) throws Exception {
        if (prepareTables) prepareDBTables(ctx);
    }

    public static void prepareDBTables(Context ctx) throws Exception {
        DBStorage storage = ctx.getEngine().getDBStorageForApplication(ExpressService.STORAGE_ID);

        storage.prepareTable(
                APP_TABLE,
                "{" +
                        "    \"fields\": {" +
                        "        \"id\": \"String[50]\"," +
                        "        \"title\": \"String[50]\"," +
                        "        \"icon\": \"String[200]\"," +
                        "        \"desc\": \"String[5000]\"," +
                        "        \"autoStart\": \"Boolean\"," +
                        "        \"abstract\": \"Boolean\"," +
                        "        \"createTime\": \"String[20]\"," +
                        "        \"updateTime\": \"String[20]\"" +
                        "    }," +
                        "    \"indexes\": [" +
                        "        {\"field\": \"id\", \"primary\": true}," +
                        "        {\"field\": \"updateTime\"}" +
                        "    ]" +
                        "}",
                ctx
        );
    }

    public static boolean hasApplication(String id, Context ctx) throws Exception {
        FileStorage storage = ctx.getEngine().getFileStorageForApplication(ExpressService.STORAGE_ID);
        String path = ROOT_PATH+id+".json";

        return storage.doesFileExist(path, ctx);
    }

    public static String getApplicationLocalStatus(String id, Context ctx) {
        ExecStatus status = ctx.getEngine().getApplicationStatus(id);

        switch (status) {
            case None: return STATUS_NONE;
            case Running: return STATUS_RUN;
            default: return STATUS_STOP;
        }
    }

    public static DataObject listCategories(Context ctx) throws Exception {
        return TagService.listTags(TagService.TagType_App_Category, ctx);
    }

    public static DataObject listProtocols(Context ctx) throws Exception {
        DataObject usedProtocols = TagService.listTags(TagService.TagType_App_Protocol, ctx);

        Map<String,Object> query = new HashMap<>();
        query.put("abstractStr", "true");
        DataObject allProtocols = queryApplications(query, "id", 0, 1000, ctx);

        JSONObject result = usedProtocols==null ? new JSONObject() : usedProtocols.getJSONObject();
        JSONArray protocols = allProtocols==null ? new JSONArray() : allProtocols.getJSONArray();

        if (protocols!=null && protocols.size()>0) {
            for (int i = 0; i < protocols.size(); i++) {
                String id = protocols.getJSONObject(i).getString("id");
                if (! result.containsKey(id)) result.put(id,0);
            }
        }

        return new DataObject(result);
    }

    public static DataObject queryApplications(Map<String,Object> query, String fields, long from, long length, Context ctx) throws Exception {
        List<Object> params = new ArrayList<>();
        String fromWhereClause = _composeFromWhereClause(query, params);

        return _queryApplications(fromWhereClause, params, fields, from, length, ctx);
    }

    public static DataObject listApplicationsById(String ids, String fields, Context ctx) throws Exception {
        if (ids==null || ids.length()==0) return null;

        StringBuilder fromWhereClause = new StringBuilder();
        fromWhereClause.append("FROM ")
                .append(ExpressService.STORAGE_ID).append('_').append(APP_TABLE)
                .append(" WHERE ");

        List<Object> params = new ArrayList<>();
        CommonCode.composeWhereClauseFromStrings("id",ids,fromWhereClause,params);

        return _queryApplications(fromWhereClause.toString(), params, fields, 0, 1000, ctx);
    }

    public static long countApplications(Map<String,Object> query, Context ctx) throws Exception {
        List<Object> params = new ArrayList<>();
        String fromWhereClause = _composeFromWhereClause(query, params);
        String sql = "SELECT count(1) AS c " + fromWhereClause;

        DBStorage storage = ctx.getEngine().getDBStorageForApplication(ExpressService.STORAGE_ID);
        Record[] records = storage.freeQuery(sql, params, 0, 1, ctx);

        if (records==null || records.length==0) return 0l;
        else return records[0].getLong("c");
    }

    private static final Map<String,String> appSpecialFields = new HashMap<>();

    static {
        appSpecialFields.put("autoStart", "Boolean");
        appSpecialFields.put("abstract", "Boolean");
    }

    private static DataObject _queryApplications(String fromWhereClause, List<Object> params, String fields, long from, long length, Context ctx) throws Exception {
        boolean includeStatus = true;
        Set<String> fieldSet = CommonCode.convertStringToSet(fields, "id", "id,status,title,icon,desc,autoStart,abstract,createTime,updateTime");

        if (fieldSet.contains("status")) fieldSet.remove("status");
        else includeStatus = false;

        DBStorage storage = ctx.getEngine().getDBStorageForApplication(ExpressService.STORAGE_ID);
        JSONArray result = new JSONArray();

        String selectClause = CommonCode.convertFieldSetToSelectClause(fieldSet);
        String orderClause = fromWhereClause.length()>0 ? " " : "";
        orderClause += "ORDER BY updateTime desc";

        String sql = "SELECT "+selectClause+" "+fromWhereClause+orderClause;

        Record[] list = storage.freeQuery(sql, params, from, length, ctx);

        if (list!=null && list.length>0) {
            for (Record record : list) {
                String id = record.getString("id");
                JSONObject item = new JSONObject();

                CommonCode.copyRecordFields(record, item, fieldSet, appSpecialFields);
                if (includeStatus) item.put("status", getApplicationLocalStatus(id, ctx));

                result.add(item);
            }
        }

        return new DataObject(result);
    }

    private static String _composeFromWhereClause(Map<String,Object> query, List<Object> params) {
        String search = query==null ? null : (String) query.get("search");
        String category = query==null ? null : (String) query.get("category");
        String protocol = query==null ? null : (String) query.get("protocol");
        String abstractStr = query==null ? null : (String) query.get("abstractStr");

        StringBuilder sql = new StringBuilder();

        if (category!=null && protocol!=null) category = null; // Ignore "category" if there's "protocol"

        sql.append("FROM ").append(ExpressService.STORAGE_ID).append('_').append(APP_TABLE);

        if (category!=null || protocol!=null) {
            sql.append(" A JOIN ")
                    .append(ExpressService.STORAGE_ID).append('_').append(TagService.TAG_TABLE)
                    .append(" T ON A.id=T.objId AND T.tagType='")
                    .append(category==null? TagService.TagType_App_Protocol : TagService.TagType_App_Category)
                    .append("'");
        }

        if (search!=null || category!=null || protocol!=null || abstractStr!=null)
            sql.append(" WHERE ");

        boolean[] hasWhere = new boolean[]{false};
        CommonCode.composeWhereClause(hasWhere, "title", search, "like", sql, params);
        CommonCode.composeWhereClause(hasWhere, "T.tag", category, sql, params);
        CommonCode.composeWhereClause(hasWhere, "T.tag", protocol, sql, params);
        CommonCode.composeWhereClause(hasWhere, "abstract", abstractStr, "boolean", sql, params);

        return sql.length()==0 ? null : sql.toString();
    }

    public static DataObject fetchApplication(String id, Context ctx) throws Exception {
        FileStorage storage = ctx.getEngine().getFileStorageForApplication(ExpressService.STORAGE_ID);
        String path = ROOT_PATH+id+".json";

        if (storage.doesFileExist(path, ctx)) {
            byte[] content = storage.readAllFromFile(path, ctx);
            if (content == null || content.length == 0) return null;
            else return new DataObject(new String(content, StandardCharsets.UTF_8));
        }
        else return null;
    }

    public static void saveApplication(String id, DataObject app, boolean categoryChanged, boolean protocolChanged, String tableSchemaChanged, Context ctx) throws Exception {
        // If only schemas saved, the application's updateTime will not be updated. To be optimized.

        Engine engine = ctx.getEngine();

        if (app == null) return;
        JSONObject appObj = app.getJSONObject();

        String updateTime = CommonCode.convertDatetimeString();
        String createTime = appObj.getString("createTime");
        if (createTime==null || createTime.length()==0) createTime = updateTime;
        appObj.put("createTime", createTime);
        appObj.put("updateTime", updateTime);

        // Save to file

        FileStorage fStorage = engine.getFileStorageForApplication(ExpressService.STORAGE_ID);
        String path = ROOT_PATH+id+".json";

        String strApp = appObj.toJSONString();
        fStorage.saveToFile(path, strApp.getBytes(StandardCharsets.UTF_8), ctx);

        // Save to DB table "application"

        String[] fields = new String[]{"id","title","icon","desc","autoStart","abstract","createTime","updateTime"};
        Record record = CommonCode.convertJSONObjectToRecord(appObj, fields);
        if (record == null) return;

        DBStorage dbStorage = engine.getDBStorageForApplication(ExpressService.STORAGE_ID);
        dbStorage.upsertRecordById(APP_TABLE, "id", record, ctx);

        // Save to DB table "tag"

        if (categoryChanged) {
            JSONArray arr = appObj.getJSONArray("categories");
            TagService.saveTags(id, TagService.TagType_App_Category, new DataObject(arr), ctx);
        }

        if (protocolChanged) {
            JSONArray arr = appObj.getJSONArray("protocols");
            TagService.saveTags(id, TagService.TagType_App_Protocol, new DataObject(arr), ctx);
        }

        // Apply the changed table schema

        if (tableSchemaChanged != null) {
            JSONArray tables = appObj.getJSONArray("tables");
            DBStorage storage = engine.getDBStorageForApplication(id);
            CommonCode.prepareTables(tables, tableSchemaChanged, storage, ctx);
        }
    }

    public static void removeApplication(String id, Context ctx) throws Exception {
        Engine engine = ctx.getEngine();
        Application app = engine.getApplication(id);

        // Remove connections
        ConnectionService.removeConnections(id, null, ctx);

        // Unregister from Phusion engine

        if (app != null) {
            if (app.getStatus().equals(ExecStatus.Running)) app.stop(ctx);
            engine.removeApplication(id, ctx);
        }

        // Unregister the Java module
        // Not do it, for there may be other components use the same module. To be optimized.

//        DataObject appDisk = fetchApplication(id, ctx);
//        if (appDisk == null) return;
//
//        JSONObject appObj = appDisk.getJSONObject();
//        JSONObject code = appObj.getJSONObject("code");
//        if (code != null) {
//            String module = code.getString("module");
//            if (module != null) engine.unloadJavaModule(module, ctx);
//        }

        // Remove the schemas
        SchemaService.removeSchemas(id, ctx);

        // Remove the application file

        FileStorage fStorage = engine.getFileStorageForApplication(ExpressService.STORAGE_ID);
        String path = ROOT_PATH+id+".json";

        fStorage.removeFile(path, ctx);

        // Remove from DB

        DBStorage dbStorage = engine.getDBStorageForApplication(ExpressService.STORAGE_ID);
        dbStorage.deleteRecordById(APP_TABLE, "id", id, ctx);

        TagService.removeTags(id, ctx);

        // Remove from cluster
        ClusterService.removeObject(ClusterService.OBJECT_APPLICATION, id, ctx);
    }

    /**
     * Save application schemas found in "obj", and remove those schemas from it.
     */
    public static void saveApplicationSchemasFromObject(String applicationId, JSONObject obj, Context ctx) throws Exception {
        SchemaService.saveSchemasFromObject(applicationId, obj, new String[]{"configSchema","connectionConfigSchema"}, ctx);
    }

    /**
     * Save endpoint schemas found in "obj", and remove those schemas from it.
     */
    public static void saveEndpointSchemasFromObject(String applicationId, String endpointId, JSONObject obj, Context ctx) throws Exception {
        SchemaService.saveSchemasFromObject(applicationId+"."+endpointId, obj,
                new String[]{"inputMessageSchema","outputMessageSchema"}, ctx);
    }

    public static void fetchSchemas(JSONObject appObj, Context ctx) throws Exception {
        String applicationId = appObj.getString("id");
        SchemaService.fetchSchemasIntoObject(applicationId, appObj, new String[]{"configSchema","connectionConfigSchema"}, ctx);

        // Fetch endpoint schemas

        applicationId += ".";
        JSONArray endpoints = appObj.getJSONArray("endpoints");
        String[] fields = new String[]{"inputMessageSchema","outputMessageSchema"};

        if (endpoints!=null && endpoints.size()>0) {
            for (Object endpoint : endpoints) {
                JSONObject obj = (JSONObject) endpoint;
                SchemaService.fetchSchemasIntoObject(applicationId+obj.getString("id"), obj, fields, ctx);
            }
        }
    }

    public static void fetchEndpointSchemasIntoObject(String applicationId, String endpointId, JSONObject obj, Context ctx) throws Exception {
        SchemaService.fetchSchemasIntoObject(applicationId+"."+endpointId, obj,
                new String[]{"inputMessageSchema","outputMessageSchema"}, ctx);
    }

    public static void removeEndpointSchemas(String applicationId, String endpointId, Context ctx) throws Exception {
        SchemaService.removeSchemas(applicationId+"."+endpointId, ctx);
    }

    public static void startApplication(String id, Context ctx) throws Exception {
        DataObject appDisk = fetchApplication(id, ctx);
        if (appDisk == null) return;
        _startApplication(id, appDisk.getJSONObject(), ctx.getEngine(), ctx);
    }

    private static void _startApplication(String id, JSONObject appObj, Engine engine, Context ctx) throws Exception {
        Application app = engine.getApplication(id);
        ExecStatus status = app==null ? ExecStatus.None : app.getStatus();

        if (status.equals(ExecStatus.None)) {
            // Load the Java module

            JSONObject code = appObj.getJSONObject("code");
            String module = null;
            String moduleClass = null;
            boolean moduleNotDefined = false;

            if (appObj.getBooleanValue("abstract",false))
                throw new PhusionException("APP_OP", "Can not start abstract applications", "applicationId="+id);

            if (code == null) moduleNotDefined = true;
            else {
                module = code.getString("module");
                moduleClass = code.getString("class");
                if (module==null || moduleClass==null || module.length()==0 || moduleClass.length()==0)
                    moduleNotDefined = true;
            }

            if (moduleNotDefined)
                throw new PhusionException("APP_OP", "Failed to start application, no modules defined", "applicationId="+id);

            ModuleService.loadJavaModule(module, ctx);

            // Prepare DB tables

//            JSONArray tables = appObj.getJSONArray("tables");
//            DBStorage storage = engine.getDBStorageForApplication(id);
//            CommonCode.prepareTables(tables, storage, ctx);

            // Register and start application

            engine.registerApplication(id, module, moduleClass, new DataObject(appObj.getJSONObject("config")), ctx);
            app = engine.getApplication(id);

            app.start(ctx);
            ClusterService.updateObjectStatus(ClusterService.OBJECT_APPLICATION, id, ApplicationService.STATUS_RUN, ctx);

            // Start connections
            ConnectionService.startConnections(id, ctx);
        }
        else if (status.equals(ExecStatus.Stopped)) {
            app.start(ctx);
            ConnectionService.startConnections(id, true, ctx);
            ClusterService.updateObjectStatus(ClusterService.OBJECT_APPLICATION, id, ApplicationService.STATUS_RUN, ctx);
        }
    }

    public static void stopApplication(String id, Context ctx) throws Exception {
        Engine engine = ctx.getEngine();
        Application app = engine.getApplication(id);

        if (app!=null && app.getStatus().equals(ExecStatus.Running)) {
            ConnectionService.stopConnections(id, ctx);
            app.stop(ctx);
            ClusterService.updateObjectStatus(ClusterService.OBJECT_APPLICATION, id, ApplicationService.STATUS_STOP, ctx);
        }
    }

    public static void restartApplication(String id, Context ctx) throws Exception {
        stopApplication(id, ctx);

        // Unregister application

        Engine engine = ctx.getEngine();
        ExecStatus status = engine.getApplicationStatus(id);
        if (status.equals(ExecStatus.Stopped)) engine.removeApplication(id, ctx);

        // Unregister the Java module

        DataObject appDisk = fetchApplication(id, ctx);
        if (appDisk == null) return;

        JSONObject appObj = appDisk.getJSONObject();
        JSONObject code = appObj.getJSONObject("code");
        engine.unloadJavaModule(code.getString("module"), ctx);

        _startApplication(id, appObj, engine, ctx);
    }

    public static void performAction(String action, String id, Context ctx) throws Exception {
        switch (action) {
            case ACTION_START: startApplication(id, ctx); break;
            case ACTION_STOP: stopApplication(id, ctx); break;
            case ACTION_RESTART: restartApplication(id, ctx); break;
        }
    }

    /**
     * Expand the list of applications to include their protocol applications.
     *
     * apps: applicationId -> JSONObject
     */
    public static void expandApplicationProtocols(Map<String,Object> apps, Context ctx) throws Exception {
        if (apps == null) return;

        Set<String> appIds = apps.keySet();
        if (appIds.size() == 0) return;

        List<Object> params = new ArrayList<>();
        params.add(TagService.TagType_App_Protocol);

        StringBuilder qmarks = new StringBuilder();

        for (String appId : appIds) {
            params.add(appId);
            if (qmarks.length() > 0) qmarks.append(',');
            qmarks.append('?');
        }

        String select = "objId,tag";
        String where = "tagType=? AND objId IN (" + qmarks + ")";
        DBStorage storage = ctx.getEngine().getDBStorageForApplication(ExpressService.STORAGE_ID);

        Record[] records = storage.queryRecords(TagService.TAG_TABLE, select, where, params, ctx);
        for (Record record : records) {
            String app = record.getString("objId");
            String protocol = record.getString("tag");
            if (! apps.containsKey(protocol)) apps.put(protocol, apps.get(app));
        }
    }

    /**
     * Start all applications whose autoStart is true.
     */
    public static void startAllApplications(Context ctx) throws Exception {
        Engine engine = ctx.getEngine();
        FileStorage storage = engine.getFileStorageForApplication(ExpressService.STORAGE_ID);

        if (storage.doesFileExist(ROOT_PATH, ctx)) {
            String[] apps = storage.listFiles(ROOT_PATH, ctx);

            if (apps!=null && apps.length>0) {
                for (String app : apps) {
                    String id = app.substring(0,app.lastIndexOf(".json"));

                    try {
                        String path = ROOT_PATH+app;
                        if (storage.doesFileExist(path, ctx)) {
                            byte[] content = storage.readAllFromFile(path, ctx);
                            if (content != null && content.length > 0) {
                                JSONObject appObj = JSON.parseObject(new String(content, StandardCharsets.UTF_8));
                                boolean autoStart = appObj.getBooleanValue("autoStart", false);

                                if (autoStart) _startApplication(id, appObj, engine, ctx);
                            }
                        }
                    } catch (Exception ex) {
                        ServiceLogger.error(_position, "Failed to start application", "applicationId="+id+", traceId="+ctx.getId(), ex);
                    }
                }
            }
        }
    }

}
