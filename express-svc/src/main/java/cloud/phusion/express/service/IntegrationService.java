package cloud.phusion.express.service;

import cloud.phusion.*;
import cloud.phusion.express.ExpressService;
import cloud.phusion.express.util.CommonCode;
import cloud.phusion.express.util.ServiceLogger;
import cloud.phusion.express.util.TimeMarker;
import cloud.phusion.integration.Integration;
import cloud.phusion.integration.IntegrationDefinition;
import cloud.phusion.integration.Transaction;
import cloud.phusion.storage.DBStorage;
import cloud.phusion.storage.FileStorage;
import cloud.phusion.storage.Record;
import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONArray;
import com.alibaba.fastjson2.JSONObject;

import java.nio.charset.StandardCharsets;
import java.util.*;

public class IntegrationService {
    private static final String _position = IntegrationService.class.getName();

    public static final String INTEGRATION_TABLE = "integration";
    public static final String INTEGRATION_APP_TABLE = "integration_application";

    private static final String ROOT_PATH = "/integration/";

    public static final String STATUS_NONE = "unused";
    public static final String STATUS_STOP = "stopped";
    public static final String STATUS_RUN = "running";

    public static final String ACTION_START = "start";
    public static final String ACTION_STOP = "stop";
    public static final String ACTION_RESTART = "restart";
    public static final String ACTION_CONFIG = "updateConfig";
    public static final String ACTION_MSG = "updateMsg";

    public static void init(boolean prepareTables, Context ctx) throws Exception {
        if (prepareTables) prepareDBTables(ctx);
    }

    public static void prepareDBTables(Context ctx) throws Exception {
        DBStorage storage = ctx.getEngine().getDBStorageForApplication(ExpressService.STORAGE_ID);

        storage.prepareTable(
                INTEGRATION_TABLE,
                "{" +
                        "    \"fields\": {" +
                        "        \"id\": \"String[50]\"," +
                        "        \"title\": \"String[50]\"," +
                        "        \"clientId\": \"String[50]\"," +
                        "        \"desc\": \"String[5000]\"," +
                        "        \"template\": \"String[50]\"," +
                        "        \"autoStart\": \"Boolean\"," +
                        "        \"abstract\": \"Boolean\"," +
                        "        \"createTime\": \"String[20]\"," +
                        "        \"updateTime\": \"String[20]\"" +
                        "    }," +
                        "    \"indexes\": [" +
                        "        {\"field\": \"id\", \"primary\": true}," +
                        "        {\"field\": \"template\"}," +
                        "        {\"field\": \"clientId\"}," +
                        "        {\"field\": \"updateTime\"}" +
                        "    ]" +
                        "}",
                ctx
        );

        storage.prepareTable(
                INTEGRATION_APP_TABLE,
                "{" +
                        "    \"fields\": {" +
                        "        \"integrationId\": \"String[50]\"," +
                        "        \"applicationId\": \"String[50]\"," +
                        "        \"connectionId\": \"String[50]\"" +
                        "    }," +
                        "    \"indexes\": [" +
                        "        {\"field\": \"integrationId\"}," +
                        "        {\"field\": \"applicationId\"}," +
                        "        {\"field\": \"connectionId\"}" +
                        "    ]" +
                        "}",
                ctx
        );
    }

    public static boolean hasIntegration(String id, Context ctx) throws Exception {
        FileStorage storage = ctx.getEngine().getFileStorageForApplication(ExpressService.STORAGE_ID);
        String path = ROOT_PATH+id+".json";

        return storage.doesFileExist(path, ctx);
    }

    public static String getIntegrationLocalStatus(String id, Context ctx) {
        ExecStatus status = ctx.getEngine().getIntegrationStatus(id);

        switch (status) {
            case None: return STATUS_NONE;
            case Running: return STATUS_RUN;
            default: return STATUS_STOP;
        }
    }

    public static DataObject listCategories(Context ctx) throws Exception {
        return TagService.listTags(TagService.TagType_It_Category, ctx);
    }

    private static final String sqlTemplates = "SELECT T.id AS id, count(I.id) AS c FROM " +
            ExpressService.STORAGE_ID+"_"+INTEGRATION_TABLE + " T LEFT JOIN " +
            ExpressService.STORAGE_ID+"_"+INTEGRATION_TABLE + " I ON T.id=I.template " +
            "WHERE T.abstract=1 GROUP BY T.id";

    public static DataObject listTemplates(Context ctx) throws Exception {
        DBStorage storage = ctx.getEngine().getDBStorageForApplication(ExpressService.STORAGE_ID);
        JSONObject result = new JSONObject();

        Record[] list = storage.freeQuery(sqlTemplates, null, 0, 1000, ctx);

        if (list!=null && list.length>0) {
            for (Record record : list) {
                String id = record.getString("id");
                if (id == null) id = "";
                result.put(id, record.getInteger("c"));
            }
        }

        return new DataObject(result);
    }

    public static DataObject queryIntegrations(Map<String,Object> query, String fields, long from, long length, Context ctx) throws Exception {
        List<Object> params = new ArrayList<>();
        String fromWhereClause = _composeFromWhereClause(query, fields, params);

        return _queryIntegrations(fromWhereClause, params, fields, from, length, ctx);
    }

    public static DataObject listIntegrationsById(String ids, String fields, Context ctx) throws Exception {
        if (ids==null || ids.length()==0) return null;

        StringBuilder fromWhereClause = new StringBuilder();
        fromWhereClause.append("FROM ");
        if (fields==null || fields.length()==0 || fields.contains("applications"))
            fromWhereClause.append(ExpressService.STORAGE_ID).append('_').append(INTEGRATION_TABLE)
                    .append(" I LEFT JOIN ")
                    .append(ExpressService.STORAGE_ID).append('_').append(INTEGRATION_APP_TABLE)
                    .append(" A ON A.integrationId=I.id");
        else
            fromWhereClause.append(ExpressService.STORAGE_ID).append('_').append(INTEGRATION_TABLE);
        fromWhereClause.append(" WHERE ");

        List<Object> params = new ArrayList<>();
        CommonCode.composeWhereClauseFromStrings("id",ids,fromWhereClause,params);

        return _queryIntegrations(fromWhereClause.toString(), params, fields, 0, 1000, ctx);
    }

    public static long countIntegrations(Map<String,Object> query, Context ctx) throws Exception {
        List<Object> params = new ArrayList<>();
        String fromWhereClause = _composeFromWhereClause(query, "id", params);
        String sql = "SELECT count(1) AS c " + fromWhereClause;

        DBStorage storage = ctx.getEngine().getDBStorageForApplication(ExpressService.STORAGE_ID);
        Record[] records = storage.freeQuery(sql, params, 0, 1, ctx);

        if (records==null || records.length==0) return 0l;
        else return records[0].getLong("c");
    }

    public static long countIntegrations(Map<String,Object> query, boolean activeOnly, Context ctx) throws Exception {
        if (!activeOnly) return countIntegrations(query, ctx);

        DataObject result = queryIntegrations(query, "id,status", 0, 1000, ctx);
        JSONArray its = result==null ? null : result.getJSONArray();
        long count = 0;

        for (int i=0; i<its.size(); i++) {
            JSONObject it = its.getJSONObject(i);
            String status = it.getString("status");
            if (status!=null && status.equals(STATUS_RUN)) count++;
        }

        return count;
    }

    private static final Map<String,String> itSpecialFields = new HashMap<>();

    static {
        itSpecialFields.put("autoStart", "Boolean");
        itSpecialFields.put("abstract", "Boolean");
    }

    private static DataObject _queryIntegrations(String fromWhereClause, List<Object> params, String fields, long from, long length, Context ctx) throws Exception {
        boolean includeStatus = true;
        boolean includeApps = true;
        Set<String> fieldSet = CommonCode.convertStringToSet(fields, "id", "id,status,title,clientId,desc,autoStart,abstract,template,applications,createTime,updateTime");

        if (fieldSet.contains("status")) fieldSet.remove("status");
        else includeStatus = false;
        if (fieldSet.contains("applications")) fieldSet.remove("applications");
        else includeApps = false;

        DBStorage storage = ctx.getEngine().getDBStorageForApplication(ExpressService.STORAGE_ID);
        JSONArray result = new JSONArray();
        Map<String,JSONObject> its = new HashMap<>(); // integrationId -> JSONObject

        String selectClause = CommonCode.convertFieldSetToSelectClause(fieldSet);
        selectClause += includeApps ? ",A.applicationId AS applicationId,A.connectionId AS connectionId" : "";
        String orderClause = fromWhereClause.length()>0 ? " " : "";
        orderClause += includeApps ? "ORDER BY I.updateTime desc" : "ORDER BY updateTime desc";

        String sql = "SELECT "+selectClause+" "+fromWhereClause+orderClause;

        Record[] list = storage.freeQuery(sql, params, from, length, ctx);

        if (list!=null && list.length>0) {
            for (Record record : list) {
                String id = record.getString("id");
                JSONObject item;

                if (! includeApps || includeApps && ! its.containsKey(id)) {
                    item = new JSONObject();

                    CommonCode.copyRecordFields(record, item, fieldSet, itSpecialFields);
                    if (includeStatus) item.put("status", getIntegrationLocalStatus(id, ctx));

                    result.add(item);
                    if (includeApps) its.put(id, item);
                }
                else
                    item = its.get(id);

                if (includeApps) {
                    String appId = record.getString("applicationId");
                    String connId = record.getString("connectionId");
                    JSONArray apps;

                    if (appId!=null && appId.length()>0) {
                        if (item.containsKey("applications"))
                            apps = item.getJSONArray("applications");
                        else {
                            apps = new JSONArray();
                            item.put("applications", apps);
                        }

                        JSONObject app = new JSONObject();
                        app.put("id", appId);
                        app.put("connectionId", connId);
                        apps.add(app);
                    }
                }
            }
        }

        return new DataObject(result);
    }

    private static String _composeFromWhereClause(Map<String,Object> query, String fields, List<Object> params) {
        String search = query==null ? null : (String) query.get("search");
        String category = query==null ? null : (String) query.get("category");
        String clientId = query==null ? null : (String) query.get("clientId");
        String applicationId = query==null ? null : (String) query.get("applicationId");
        String connectionId = query==null ? null : (String) query.get("connectionId");
        String template = query==null ? null : (String) query.get("template");
        String abstractStr = query==null ? null : (String) query.get("abstractStr");

        StringBuilder sql = new StringBuilder();
        boolean includeApps = (fields==null || fields.length()==0 || fields.contains("applications"));

        sql.append("FROM ").append(ExpressService.STORAGE_ID).append('_').append(INTEGRATION_TABLE);

        if (category != null)
            sql.append(" I JOIN ")
                    .append(ExpressService.STORAGE_ID).append('_').append(TagService.TAG_TABLE)
                    .append(" T ON I.id=T.objId AND T.tagType='")
                    .append(TagService.TagType_It_Category)
                    .append("'");

        if (applicationId!=null || connectionId!=null || includeApps) {
            if (category == null) sql.append(" I");
            if (includeApps) sql.append(" LEFT");
            sql.append(" JOIN ")
                    .append(ExpressService.STORAGE_ID).append('_').append(INTEGRATION_APP_TABLE)
                    .append(" A ON I.id=A.integrationId");
        }

        if (search!=null || category!=null || clientId!=null || applicationId!=null || connectionId!=null || template!=null || abstractStr!=null)
            sql.append(" WHERE ");

        boolean[] hasWhere = new boolean[]{false};
        CommonCode.composeWhereClause(hasWhere, "title", search, "like", sql, params);
        CommonCode.composeWhereClause(hasWhere, "T.tag", category, sql, params);
        CommonCode.composeWhereClause(hasWhere, "clientId", clientId, sql, params);
        CommonCode.composeWhereClause(hasWhere, "A.applicationId", applicationId, sql, params);
        CommonCode.composeWhereClause(hasWhere, "A.connectionId", connectionId, sql, params);
        CommonCode.composeWhereClause(hasWhere, "template", template, sql, params);
        CommonCode.composeWhereClause(hasWhere, "abstract", abstractStr, "boolean", sql, params);

        return sql.length()==0 ? null : sql.toString();
    }

    public static DataObject fetchIntegration(String id, Context ctx) throws Exception {
        FileStorage storage = ctx.getEngine().getFileStorageForApplication(ExpressService.STORAGE_ID);
        String path = ROOT_PATH+id+".json";

        if (storage.doesFileExist(path, ctx)) {
            byte[] content = storage.readAllFromFile(path, ctx);
            if (content == null || content.length == 0) return null;
            else return new DataObject(new String(content, StandardCharsets.UTF_8));
        }
        else return null;
    }

    public static void generateScriptFile(String id, JSONArray workflow, Context ctx) throws Exception {
        if (workflow == null) {
            // Load integration from file

            DataObject it = fetchIntegration(id, ctx);
            if (it == null) return;

            JSONObject itObj = it.getJSONObject();
            if (itObj == null) return;

            workflow = itObj.getJSONArray("workflow");
        }

        List<String> jsSteps = new ArrayList<>();

        if (workflow!=null && workflow.size()>0) {
            for (int i = 0; i < workflow.size(); i++) {
                JSONObject step = workflow.getJSONObject(i);
                if ("processor".equals(step.getString("type")) &&
                        "javascript".equals(step.getString("subtype")))
                    jsSteps.add(step.getString("id"));
            }
        }

        String code = CodeRunner.generateCodeForIntegration(jsSteps);
        String codeFile = id+"."+CodeRunner.CODE_TYPE_NODE;

        ModuleService.saveCode(ModuleService.CODE_OWNER_INTEGRATION, codeFile, new DataObject(code), ctx);
    }

    public static boolean isTemplate(String id, Context ctx) throws Exception {
        DBStorage storage = ctx.getEngine().getDBStorageForApplication(ExpressService.STORAGE_ID);
        Record record = storage.queryRecordById(INTEGRATION_TABLE, "abstract", "id", id, ctx);
        if (record == null) return false;

        Boolean result = record.getBoolean("abstract");
        return result!=null && result;
    }

    public static void saveIntegration(String id, DataObject it, boolean workflowChanged, boolean categoryChanged, boolean applicationChanged, boolean generateScript, String tableSchemaChanged, Context ctx) throws Exception {
        // If only schemas saved, the integration's updateTime will not be updated. To be optimized.

        Engine engine = ctx.getEngine();

        if (it == null) return;
        JSONObject itObj = it.getJSONObject();

        String updateTime = CommonCode.convertDatetimeString();
        String createTime = itObj.getString("createTime");
        if (createTime==null || createTime.length()==0) createTime = updateTime;
        itObj.put("createTime", createTime);
        itObj.put("updateTime", updateTime);

        // Fill in script module ID (i.e. integration ID)

        if (workflowChanged) {
            JSONArray workflow = itObj.getJSONArray("workflow");
            if (workflow!=null && workflow.size()>0) {
                boolean changed = false;

                for (int i = 0; i < workflow.size(); i++) {
                    JSONObject step = workflow.getJSONObject(i);
                    if ("processor".equals(step.getString("type")) &&
                            "javascript".equals(step.getString("subtype"))) {
                        if (! id.equals(step.getString("script"))) {
                            step.put("script", id);
                            changed = true;
                        }
                    }
                }

                if (changed) {
                    it.setData(itObj);
                }
            }
        }

        // Save to file

        FileStorage fStorage = engine.getFileStorageForApplication(ExpressService.STORAGE_ID);
        String path = ROOT_PATH+id+".json";

        String strIt = itObj.toJSONString();
        fStorage.saveToFile(path, strIt.getBytes(StandardCharsets.UTF_8), ctx);

        if (generateScript) {
            JSONArray workflow = itObj.getJSONArray("workflow");
            generateScriptFile(id, workflow, ctx);
        }

        // Save to DB table "integration"

        String[] fields = new String[]{"id","title","clientId","desc","template","autoStart","abstract","createTime","updateTime"};
        Record record = CommonCode.convertJSONObjectToRecord(itObj, fields);
        if (record == null) return;

        DBStorage dbStorage = engine.getDBStorageForApplication(ExpressService.STORAGE_ID);
        dbStorage.upsertRecordById(INTEGRATION_TABLE, "id", record, ctx);

        // Save to DB table "tag"

        if (categoryChanged) {
            JSONArray arr = itObj.getJSONArray("categories");
            TagService.saveTags(id, TagService.TagType_It_Category, new DataObject(arr), ctx);
        }

        // Save to DB table "integration_application"

        if (applicationChanged) {
            JSONArray arr = itObj.getJSONArray("applications");
            _saveApplications(id, new DataObject(arr), dbStorage, ctx);
        }

        // Apply the changed table schema

        if (tableSchemaChanged != null) {
            JSONArray tables = itObj.getJSONArray("tables");
            DBStorage storage = engine.getDBStorageForIntegration(id);
            CommonCode.prepareTables(tables, tableSchemaChanged, storage, ctx);
        }
    }

    private static void _saveApplications(String id, DataObject apps, DBStorage storage, Context ctx) throws Exception {
        // Not in DB transaction, to be optimized.

        _removeApplications(id, storage, ctx); // Remove the previous applications

        if (apps == null) return;

        JSONArray arr = apps.getJSONArray();

        if (arr.size() == 0) return;

        List<Object> params = new ArrayList<>();
        Set<String> appSet = new HashSet<>();

        for (int i = 0; i < arr.size(); i++) {
            JSONObject app = arr.getJSONObject(i);
            String appId = app.getString("id");
            String connId = app.getString("connectionId");
            if (appSet.contains(appId)) continue;

            params.add(id);
            params.add(appId);
            params.add(connId);

            appSet.add(appId);
        }

        storage.insertRecords(INTEGRATION_APP_TABLE, "integrationId,applicationId,connectionId", params, ctx);
    }

    private static void _removeApplications(String id, DBStorage storage, Context ctx) throws Exception {
        String where = "integrationId=?";
        List<Object> params = new ArrayList<>();
        params.add(id);

        storage.deleteRecords(INTEGRATION_APP_TABLE, where, params, ctx);
    }

    public static void removeIntegration(String id, Context ctx) throws Exception {
        Engine engine = ctx.getEngine();
        Integration it = engine.getIntegration(id);

        // Unregister from Phusion engine

        if (it != null) {
            if (it.getStatus().equals(ExecStatus.Running)) it.stop(ctx);
            engine.removeIntegration(id, ctx);
        }

        // Unregister the Java module
        // Not do it, for there may be other components use the same module. To be optimized.

//        DataObject itDisk = fetchIntegration(id, ctx);
//        if (itDisk == null) return;
//
//        JSONObject itObj = itDisk.getJSONObject();
//        if (itObj != null) {
//            JSONArray workflow = itObj.getJSONArray("workflow");
//            _loadJavaModules(id, workflow, false, ctx);
//        }

        // Unregister and remove the JavaScript module (there must be no integrations using this module)

        _loadJavaScriptModule(id, false, ctx);
        ModuleService.removeCode(ModuleService.CODE_OWNER_INTEGRATION, id+"."+CodeRunner.CODE_TYPE_NODE, ctx);

        // Remove the schemas
        SchemaService.removeSchemas(id, ctx);

        // Remove the integration file

        FileStorage fStorage = engine.getFileStorageForApplication(ExpressService.STORAGE_ID);
        String path = ROOT_PATH+id+".json";

        fStorage.removeFile(path, ctx);

        // Remove from DB

        DBStorage dbStorage = engine.getDBStorageForApplication(ExpressService.STORAGE_ID);
        dbStorage.deleteRecordById(INTEGRATION_TABLE, "id", id, ctx);

        _removeApplications(id, dbStorage, ctx);

        TagService.removeTags(id, ctx);

        // Remove from cluster
        ClusterService.removeObject(ClusterService.OBJECT_INTEGRATION, id, ctx);
    }

    private static void _loadJavaModules(String id, JSONArray workflow, boolean loading, Context ctx) throws Exception {
        Engine engine = ctx.getEngine();

        if (workflow!=null && workflow.size()>0) {
            for (int i = 0; i < workflow.size(); i++) {
                JSONObject step = workflow.getJSONObject(i);
                if ("processor".equals(step.getString("type")) &&
                        "java".equals(step.getString("subtype"))) {

                    String module = step.getString("module");
                    String moduleClass = step.getString("class");

                    if (loading) {
                        if (module==null || moduleClass==null || module.length()==0 || moduleClass.length()==0) {
                            throw new PhusionException("IT_OP", "Failed to start integration, no modules defined for Java steps",
                                    "integrationId="+id+", step="+step.getString("id"));
                        }

                        if (! engine.doesJavaModuleExist(module)) ModuleService.loadJavaModule(module, ctx);
                    }
                    else {
                        if (module!=null && engine.doesJavaModuleExist(module)) engine.unloadJavaModule(module, ctx);
                    }
                }
            }
        }
    }

    private static void _loadJavaScriptModule(String id, boolean loading, Context ctx) throws Exception {
        // JavaScript module ID = Integration ID

        Engine engine = ctx.getEngine();

        if (loading) {
            String file = ModuleService.CODE_OWNER_INTEGRATION+"/"+id+"."+CodeRunner.CODE_TYPE_NODE;
            if (! engine.doesJavaScriptModuleExist(id)) engine.loadJavaScriptModule(id, file, ctx);
        }
        else {
            if (engine.doesJavaScriptModuleExist(id)) engine.unloadJavaScriptModule(id, ctx);
        }
    }

    /**
     * Save integration schemas found in "obj", and remove those schemas from it.
     */
    public static void saveIntegrationSchemasFromObject(String id, JSONObject obj, Context ctx) throws Exception {
        SchemaService.saveSchemasFromObject(id, obj, new String[]{"configSchema"}, ctx);
    }

    public static void fetchSchemas(JSONObject itObj, Context ctx) throws Exception {
        String id = itObj.getString("id");
        SchemaService.fetchSchemasIntoObject(id, itObj, new String[]{"configSchema"}, ctx);
    }

    public static void startIntegration(String id, Context ctx) throws Exception {
        DataObject itDisk = fetchIntegration(id, ctx);
        if (itDisk == null) return;
        _startIntegration(id, itDisk.getJSONObject(), ctx.getEngine(), ctx);
    }

    private static void _startIntegration(String id, JSONObject itObj, Engine engine, Context ctx) throws Exception {
        Integration it = engine.getIntegration(id);
        ExecStatus status = it==null ? ExecStatus.None : it.getStatus();

        if (status.equals(ExecStatus.None)) {
            it = _loadIntegration(id, itObj, engine, ctx);

            it.start(ctx);
            ClusterService.updateObjectStatus(ClusterService.OBJECT_INTEGRATION, id, IntegrationService.STATUS_RUN, ctx);
        }
        else if (status.equals(ExecStatus.Stopped)) {
            it.start(ctx);
            ClusterService.updateObjectStatus(ClusterService.OBJECT_INTEGRATION, id, IntegrationService.STATUS_RUN, ctx);
        }
    }

    private static Integration _loadIntegration(String id, JSONObject itObj, Engine engine, Context ctx) throws Exception {
        if (itObj.getBooleanValue("abstract",false))
            throw new PhusionException("IT_OP", "Can not start abstract integrations", "integrationId="+id);

        // If there's a template, use its workflow, timer and startCondition

        String template = itObj.getString("template");
        if (template!=null && template.length()==0) template = null;
        JSONArray workflow = itObj.getJSONArray("workflow");
        JSONObject timer = itObj.getJSONObject("timer");
        JSONObject startCondition = itObj.getJSONObject("startCondition");

        if (template != null) {
            DataObject tmp = IntegrationService.fetchIntegration(template, ctx);
            JSONObject tmpObj = tmp==null ? null : tmp.getJSONObject();
            if (tmpObj != null) {
                workflow = tmpObj.getJSONArray("workflow");
                timer = tmpObj.getJSONObject("timer");
                startCondition = tmpObj.getJSONObject("startCondition");
            }
        }

        // Load the Java and JavaScript modules

        _loadJavaModules(id, workflow, true, ctx);
        _loadJavaScriptModule(template==null ? id : template, true, ctx);

        // Prepare DB tables

//        JSONArray tables = itObj.getJSONArray("tables");
//        DBStorage storage = engine.getDBStorageForIntegration(id);
//        CommonCode.prepareTables(tables, storage, ctx);

        // Register and start integration

        String clientId = itObj.getString("clientId");
        JSONObject configObj = itObj.getJSONObject("config");
        DataObject config = configObj==null ? null : new DataObject(configObj);
        IntegrationDefinition idef = _getIntegrationDef(itObj, workflow, timer, startCondition, ctx);

        engine.registerIntegration(id, clientId, idef, config, ctx);
        return engine.getIntegration(id);
    }

    private static IntegrationDefinition _getIntegrationDef(JSONObject itObj, JSONArray workflow, JSONObject timer, JSONObject startCondition, Context ctx) throws Exception {
        IntegrationDefinition idef = new IntegrationDefinition();

        // Fill up endpoints' application ID and connection ID

        JSONArray appConns = itObj.getJSONArray("applications");
        if (appConns!=null && appConns.size()>0 && workflow!=null && workflow.size()>0) {
            Map<String, Object> appConnsObj = CommonCode.convertJSONArrayToMap(appConns, "id");
            ApplicationService.expandApplicationProtocols(appConnsObj, ctx);

            for (int i = 0; i < workflow.size(); i++) {
                JSONObject step = workflow.getJSONObject(i);
                if ("endpoint".equals(step.getString("type"))) {
                    Object app = appConnsObj.get(step.getString("app"));
                    if (app != null) {
                        JSONObject appObj = (JSONObject) app;
                        step.put("app", appObj.getString("id"));
                        step.put("connection", appObj.getString("connectionId"));
                    }
                }
            }
        }

        idef.setWorkflow(workflow);
        idef.setStartCondition(startCondition);

        // Setup timer

        if (timer != null) {
            String cron = timer.getString("cron");
            int interval = timer.getIntValue("interval", 0);
            boolean clustered = timer.getBooleanValue("clustered", false);

            if (cron!=null && cron.length()>0)
                idef.setCronSchedule(cron, clustered);
            else
                idef.setPeriodicSchedule(interval, 0, null, clustered);
        }

        return idef;
    }

    public static void stopIntegration(String id, Context ctx) throws Exception {
        Engine engine = ctx.getEngine();
        Integration it = engine.getIntegration(id);

        if (it!=null && it.getStatus().equals(ExecStatus.Running)) {
            it.stop(ctx);
            ClusterService.updateObjectStatus(ClusterService.OBJECT_INTEGRATION, id, IntegrationService.STATUS_STOP, ctx);
        }
    }

    public static void restartIntegration(String id, Context ctx) throws Exception {
        stopIntegration(id, ctx);

        // Unregister integration

        Engine engine = ctx.getEngine();
        ExecStatus status = engine.getIntegrationStatus(id);
        if (status.equals(ExecStatus.Stopped)) engine.removeIntegration(id, ctx);

        // Unregister the Java module and JavaScript

        DataObject itDisk = fetchIntegration(id, ctx);
        if (itDisk == null) return;

        JSONObject itObj = itDisk.getJSONObject();
        JSONArray workflow = itObj.getJSONArray("workflow");
        _loadJavaModules(id, workflow, false, ctx);
        _loadJavaScriptModule(id, false, ctx);

        _startIntegration(id, itObj, engine, ctx);
    }

    public static void dynamicUpdateConfig(String id, Context ctx) throws Exception {
        Engine engine = ctx.getEngine();
        Integration it = engine.getIntegration(id);

        // If the integration is not running, need not update dynamically.
        if (it==null || ! it.getStatus().equals(ExecStatus.Running)) return;

        // Update config

        DataObject itDisk = fetchIntegration(id, ctx);
        if (itDisk == null) return;

        JSONObject config = itDisk.getJSONObject();
        config = config.getJSONObject("config");
        if (config == null) return;

        it.updateConfig(new DataObject(config), ctx);

        ClusterService.updateObjectStatus(ClusterService.OBJECT_INTEGRATION, id, IntegrationService.STATUS_RUN, ctx);
    }

    public static void dynamicUpdateStepMessages(String id, Context ctx) throws Exception {
        Engine engine = ctx.getEngine();
        Integration it = engine.getIntegration(id);

        // If the integration is not running, need not update dynamically.
        if (it==null || ! it.getStatus().equals(ExecStatus.Running)) return;

        // Update all direct-step messages

        DataObject itDisk = fetchIntegration(id, ctx);
        if (itDisk == null) return;

        JSONObject itObj = itDisk.getJSONObject();
        JSONArray workflow = itObj.getJSONArray("workflow");
        if (workflow==null || workflow.size()==0) return;

        for (int i = 0; i < workflow.size(); i++) {
            JSONObject step = workflow.getJSONObject(i);
            if ("direct".equals(step.getString("type"))) {
                String msg = step.getString("msg");
                it.updateStepMsg(step.getString("id"), new DataObject(msg), ctx);
            }
        }

        ClusterService.updateObjectStatus(ClusterService.OBJECT_INTEGRATION, id, IntegrationService.STATUS_RUN, ctx);
    }

    public static void saveConfig(String id, JSONObject config, Context ctx) throws Exception {
        if (config==null || config.size()==0) return;

        DataObject itDisk = fetchIntegration(id, ctx);
        if (itDisk == null) return;

        JSONObject itObj = itDisk.getJSONObject();
        JSONObject oldConfig = itObj.getJSONObject("config");
        if (oldConfig != null) {
            Set<String> oldConfigKeys = oldConfig.keySet();
            for (String key : oldConfigKeys) {
                if (! config.containsKey(key)) config.put(key, oldConfig.get(key));
            }
        }

        itObj.put("config", config);

        itDisk.setData(itObj);
        saveIntegration(id, itDisk, false, false, false, false, null, ctx);
    }

    public static void saveStepMessages(String id, JSONObject msgs, Context ctx) throws Exception {
        if (msgs==null || msgs.size()==0) return;

        DataObject itDisk = fetchIntegration(id, ctx);
        if (itDisk == null) return;

        JSONObject itObj = itDisk.getJSONObject();
        JSONArray workflow = itObj.getJSONArray("workflow");
        if (workflow==null || workflow.size()==0) return;

        for (int i = 0; i < workflow.size(); i++) {
            JSONObject step = workflow.getJSONObject(i);
            if ("direct".equals(step.getString("type"))) {
                Object msg = msgs.get( step.getString("id") );
                if (msg != null) step.put("msg", msg);
            }
        }

        itDisk.setData(itObj);
        saveIntegration(id, itDisk, false, false, false, false, null, ctx);
    }

    public static void performAction(String action, String id, Context ctx) throws Exception {
        switch (action) {
            case ACTION_START: startIntegration(id, ctx); break;
            case ACTION_STOP: stopIntegration(id, ctx); break;
            case ACTION_RESTART: restartIntegration(id, ctx); break;
            case ACTION_CONFIG: dynamicUpdateConfig(id, ctx); break;
            case ACTION_MSG: dynamicUpdateStepMessages(id, ctx); break;
        }
    }

    public static DataObject testIntegration(String id, String step, boolean failed, DataObject msg, DataObject properties, boolean moveOn, Context ctx) throws Exception {
        Engine engine = ctx.getEngine();
        Integration it = engine.getIntegration(id);

        if (it == null) {
            DataObject itDisk = fetchIntegration(id, ctx);
            if (itDisk == null) return null;

            JSONObject itObj = itDisk.getJSONObject();
            it = _loadIntegration(id, itObj, engine, ctx);
        }

        Map<String, Object> props = properties==null ? null : CommonCode.convertJSONObjectToMap(properties.getJSONObject());
        TimeMarker t = new TimeMarker();

        Transaction trx = it.createInstance(msg, step, null, failed, props, ctx);
        it.probe(trx, moveOn);

        JSONObject result = new JSONObject();
        result.put("failed", trx.isFailed());
        result.put("finished", trx.isFinished());
        result.put("duration", t.mark());
        result.put("engineId", engine.getId());

        String nextStep = trx.getCurrentStep();
        if (nextStep != null) result.put("nextStep", nextStep);

        DataObject msgResult = trx.getMessage();
        if (msgResult != null) {
            JSONObject msgObj = msgResult.getJSONObject();
            if (msgObj != null) result.put("msg", msgObj);
            else {
                JSONArray msgArr = msgResult.getJSONArray();
                if (msgArr != null) result.put("msg", msgArr);
                else result.put("msg", msgResult.getString());
            }
        }

        Map<String, Object> propsResult = trx.getProperties();
        if (propsResult !=null) result.put("properties", CommonCode.convertMapToJSONObject(propsResult));

        return new DataObject(result);
    }

    /**
     * Start all integrations whose autoStart is true.
     */
    public static void startAllIntegrations(Context ctx) throws Exception {
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
                                JSONObject itObj = JSON.parseObject(new String(content, StandardCharsets.UTF_8));
                                boolean autoStart = itObj.getBooleanValue("autoStart", false);

                                if (autoStart) _startIntegration(id, itObj, engine, ctx);
                            }
                        }
                    } catch (Exception ex) {
                        ServiceLogger.error(_position, "Failed to start integration", "integrationId="+id+", traceId="+ctx.getId(), ex);
                    }
                }
            }
        }
    }

}
