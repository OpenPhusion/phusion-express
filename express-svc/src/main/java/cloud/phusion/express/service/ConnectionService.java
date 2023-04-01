package cloud.phusion.express.service;

import cloud.phusion.*;
import cloud.phusion.application.Application;
import cloud.phusion.application.ConnectionStatus;
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
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public class ConnectionService {
    private static final String _position = ConnectionService.class.getName();

    private static final String ROOT_PATH = "/connection/";
    private static final String CONN_TABLE = "connection";

    public static final String STATUS_NONE = "unused";
    public static final String STATUS_STOP = "unconnected";
    public static final String STATUS_RUN = "connected";

    public static final String ACTION_CONNECT = "connect";
    public static final String ACTION_DISCONNECT = "disconnect";
    public static final String ACTION_RECONNECT = "reconnect";

    public static void init(boolean prepareTables, Context ctx) throws Exception {
        if (prepareTables) prepareDBTables(ctx);
    }

    public static void prepareDBTables(Context ctx) throws Exception {
        DBStorage storage = ctx.getEngine().getDBStorageForApplication(ExpressService.STORAGE_ID);

        storage.prepareTable(
                CONN_TABLE,
                "{" +
                        "    \"fields\": {" +
                        "        \"id\": \"String[50]\"," +
                        "        \"title\": \"String[50]\"," +
                        "        \"applicationId\": \"String[50]\"," +
                        "        \"clientId\": \"String[50]\"," +
                        "        \"desc\": \"String[5000]\"," +
                        "        \"createTime\": \"String[20]\"," +
                        "        \"updateTime\": \"String[20]\"" +
                        "    }," +
                        "    \"indexes\": [" +
                        "        {\"field\": \"id\", \"primary\": true}," +
                        "        {\"field\": \"applicationId\"}," +
                        "        {\"field\": \"clientId\"}," +
                        "        {\"field\": \"updateTime\"}" +
                        "    ]" +
                        "}",
                ctx
        );
    }

    public static boolean hasConnection(String id, Context ctx) throws Exception {
        FileStorage storage = ctx.getEngine().getFileStorageForApplication(ExpressService.STORAGE_ID);
        String path = ROOT_PATH+id+".json";

        return storage.doesFileExist(path, ctx);
    }

    public static String getConnectionLocalStatus(String id, String applicationId, Context ctx) throws Exception {
        Application app = ctx.getEngine().getApplication(applicationId);

        if (app == null) return STATUS_NONE;
        else {
            ConnectionStatus status = app.getConnectionStatus(id);

            switch (status) {
                case None: return STATUS_NONE;
                case Connected: return STATUS_RUN;
                default: return STATUS_STOP;
            }
        }
    }

    public static long countConnections(String applicationId, String clientId, Context ctx) throws Exception {
        Object[] clause = _composeWhereClause(applicationId, clientId);
        String whereClause = (String) clause[0];
        List<Object> params = (List<Object>) clause[1];

        Engine engine = ctx.getEngine();
        DBStorage storage = engine.getDBStorageForApplication(ExpressService.STORAGE_ID);

        return storage.queryCount(CONN_TABLE, "id", whereClause, params, ctx);
    }

    private static Object[] _composeWhereClause(String applicationId, String clientId) {
        String whereClause = null;
        List<Object> params = null;

        if (applicationId!=null || clientId!=null) {
            if (applicationId != null) {
                whereClause = "applicationId=?";
                params = new ArrayList<>();
                params.add(applicationId);
            }

            if (clientId != null) {
                whereClause = (whereClause==null ? "" : whereClause+" AND ") + "clientId=?";
                if (params == null) params = new ArrayList<>();
                params.add(clientId);
            }
        };

        return new Object[]{whereClause, params};
    }

    /**
     * return JSONArray of connections
     */
    public static DataObject listConnections(String applicationId, String clientId, String fields, Context ctx) throws Exception {
        return listConnections(applicationId,clientId,fields,0,1000,ctx);
    }

    public static DataObject listConnections(String applicationId, String clientId, String fields, long from, long length, Context ctx) throws Exception {
        Object[] clause = _composeWhereClause(applicationId, clientId);
        String whereClause = (String) clause[0];
        List<Object> params = (List<Object>) clause[1];

        return _listConnections(null, whereClause, params, fields, 0, 1000, ctx);
    }

    public static DataObject listConnectionsById(String ids, String fields, Context ctx) throws Exception {
        if (ids==null || ids.length()==0) return null;

        StringBuilder whereClause = new StringBuilder();
        List<Object> params = new ArrayList<>();
        CommonCode.composeWhereClauseFromStrings("id", ids, whereClause, params);

        return _listConnections(null, whereClause.toString(), params, fields, 0, 1000, ctx);
    }

    private static DataObject _listConnections(String applicationId, String whereClause, List<Object> params, String fields, long from, long length, Context ctx) throws Exception {
        boolean includeStatus = true;

        Set<String> fieldSet = CommonCode.convertStringToSet(fields, "id", "id,status,title,desc,applicationId,clientId,createTime,updateTime");

        if (fieldSet.contains("status")) fieldSet.remove("status");
        else includeStatus = false;

        Engine engine = ctx.getEngine();
        DBStorage storage = engine.getDBStorageForApplication(ExpressService.STORAGE_ID);
        JSONArray result = new JSONArray();

        String selectClause = CommonCode.convertFieldSetToSelectClause(fieldSet);
        String orderClause = "updateTime desc";

        Record[] list = storage.queryRecords(CONN_TABLE, selectClause, whereClause, params, orderClause, from, length, ctx);

        if (list!=null && list.length>0) {
            for (Record record : list) {
                String id = record.getString("id");
                String appId = record.getString("applicationId");
                if (appId == null) appId = applicationId;

                JSONObject item = new JSONObject();

                CommonCode.copyRecordFields(record, item, fieldSet);
                if (includeStatus) item.put("status", getConnectionLocalStatus(id, appId, ctx));

                result.add(item);
            }
        }

        return new DataObject(result);
    }

    public static DataObject fetchConnection(String id, Context ctx) throws Exception {
        FileStorage storage = ctx.getEngine().getFileStorageForApplication(ExpressService.STORAGE_ID);
        String path = ROOT_PATH+id+".json";

        if (storage.doesFileExist(path, ctx)) {
            byte[] content = storage.readAllFromFile(path, ctx);
            if (content == null || content.length == 0) return null;
            else return new DataObject(new String(content, StandardCharsets.UTF_8));
        }
        else return null;
    }

    public static void saveConnection(String id, DataObject conn, Context ctx) throws Exception {
        Engine engine = ctx.getEngine();

        // Save to file

        if (conn == null) return;
        JSONObject connObj = conn.getJSONObject();

        String updateTime = CommonCode.convertDatetimeString();
        String createTime = connObj.getString("createTime");
        if (createTime==null || createTime.length()==0) createTime = updateTime;
        connObj.put("createTime", createTime);
        connObj.put("updateTime", updateTime);

        FileStorage fStorage = engine.getFileStorageForApplication(ExpressService.STORAGE_ID);
        String path = ROOT_PATH+id+".json";

        String strConn = connObj.toJSONString();
        fStorage.saveToFile(path, strConn.getBytes(StandardCharsets.UTF_8), ctx);

        // Save to DB

        String[] fields = new String[]{"id","title","applicationId","clientId","desc","createTime","updateTime"};
        Record record = CommonCode.convertJSONObjectToRecord(conn.getJSONObject(), fields);
        if (record == null) return;

        DBStorage dbStorage = engine.getDBStorageForApplication(ExpressService.STORAGE_ID);
        dbStorage.upsertRecordById(CONN_TABLE, "id", record, ctx);
    }

    public static void removeConnection(String id, Context ctx) throws Exception {
        DataObject connDisk = fetchConnection(id, ctx);
        if (connDisk == null) return;
        _removeConnection(id, connDisk.getJSONObject().getString("applicationId"), ctx);
    }

    private static void _removeConnection(String id, String applicationId, Context ctx) throws Exception {
        Engine engine = ctx.getEngine();
        Application app = engine.getApplication(applicationId);

        // Unregister from application

        if (app != null) {
            if (app.getConnectionStatus(id).equals(ConnectionStatus.Connected)) app.disconnect(id, ctx);
            app.removeConnection(id, ctx);
        }

        // Remove the connection file

        FileStorage fStorage = engine.getFileStorageForApplication(ExpressService.STORAGE_ID);
        String path = ROOT_PATH+id+".json";

        fStorage.removeFile(path, ctx);

        // Remove from DB

        DBStorage dbStorage = engine.getDBStorageForApplication(ExpressService.STORAGE_ID);
        dbStorage.deleteRecordById(CONN_TABLE, "id", id, ctx);

        // Remove from cluster
        ClusterService.removeObject(ClusterService.OBJECT_CONNECTION, id, ctx);
    }

    public static void startConnection(String id, Context ctx) throws Exception {
        DataObject connDisk = fetchConnection(id, ctx);
        if (connDisk == null) return;

        Engine engine = ctx.getEngine();
        JSONObject connObj = connDisk.getJSONObject();

        String applicationId = connObj.getString("applicationId");
        Application app = engine.getApplication(applicationId);

        if (app != null) _startConnection(id, app, connObj, engine, ctx);
    }

    private static void _startConnection(String id, Application app, JSONObject connObj, Engine engine, Context ctx) throws Exception {
        ConnectionStatus status = app.getConnectionStatus(id);

        if (status.equals(ConnectionStatus.None)) {
            app.createConnection(id, new DataObject(connObj.getJSONObject("config")), ctx);
            app.connect(id, ctx);
            ClusterService.updateObjectStatus(ClusterService.OBJECT_CONNECTION, id, ConnectionService.STATUS_RUN, ctx);
        } else if (status.equals(ConnectionStatus.Unconnected)) {
            app.connect(id, ctx);
            ClusterService.updateObjectStatus(ClusterService.OBJECT_CONNECTION, id, ConnectionService.STATUS_RUN, ctx);
        }
    }

    public static void stopConnection(String id, Context ctx) throws Exception {
        DataObject connDisk = fetchConnection(id, ctx);
        if (connDisk == null) return;

        String applicationId = connDisk.getJSONObject().getString("applicationId");
        Application app = ctx.getEngine().getApplication(applicationId);

        if (app != null) _stopConnection(id, app, ctx);
    }

    private static void _stopConnection(String id, Application app, Context ctx) throws Exception {
        if (app!=null && app.getStatus().equals(ExecStatus.Running) &&
                app.getConnectionStatus(id).equals(ConnectionStatus.Connected)) {
            app.disconnect(id, ctx);
            ClusterService.updateObjectStatus(ClusterService.OBJECT_CONNECTION, id, ConnectionService.STATUS_STOP, ctx);
        }
    }

    public static void restartConnection(String id, Context ctx) throws Exception {
        DataObject connDisk = fetchConnection(id, ctx);
        if (connDisk == null) return;

        JSONObject connObj = connDisk.getJSONObject();
        String applicationId = connObj.getString("applicationId");

        Engine engine = ctx.getEngine();
        Application app = engine.getApplication(applicationId);
        if (app == null) return;

        _stopConnection(id, app, ctx);

        // Unregister connection
        app.removeConnection(id, ctx);

        _startConnection(id, app, connObj, engine, ctx);
    }

    public static void performAction(String action, String id, Context ctx) throws Exception {
        switch (action) {
            case ACTION_CONNECT: startConnection(id, ctx); break;
            case ACTION_DISCONNECT: stopConnection(id, ctx); break;
            case ACTION_RECONNECT: restartConnection(id, ctx); break;
        }
    }

    /**
     * Start all connections for the application.
     */
    public static void startConnections(String applicationId, Context ctx) throws Exception {
        startConnections(applicationId, false, ctx);
    }

    public static void startConnections(String applicationId, boolean updateStatusOnly, Context ctx) throws Exception {
        Engine engine = ctx.getEngine();
        Application app = engine.getApplication(applicationId);
        if (app == null) return;

        DataObject conns = listConnections(applicationId, null, "id", ctx);
        if (conns == null) return;

        JSONArray arr = conns.getJSONArray();
        if (arr==null || arr.size()==0) return;

        FileStorage storage = engine.getFileStorageForApplication(ExpressService.STORAGE_ID);

        for (Object conn : arr) {
            String id = ((JSONObject) conn).getString("id");

            if (updateStatusOnly) {
                ClusterService.updateObjectStatus(ClusterService.OBJECT_CONNECTION, id, ConnectionService.STATUS_RUN, ctx);
            }
            else {
                try {
                    String path = ROOT_PATH + id + ".json";
                    if (storage.doesFileExist(path, ctx)) {
                        byte[] content = storage.readAllFromFile(path, ctx);
                        if (content != null && content.length > 0) {
                            JSONObject connObj = JSON.parseObject(new String(content, StandardCharsets.UTF_8));
                            _startConnection(id, app, connObj, engine, ctx);
                        }
                    }
                } catch (Exception ex) {
                    ServiceLogger.error(_position, "Failed to start connection", "connectionId=" + id + ", traceId=" + ctx.getId(), ex);
                }
            }
        }
    }

    /**
     * Stop all connections for the application.
     */
    public static void stopConnections(String applicationId, Context ctx) throws Exception {
        Engine engine = ctx.getEngine();
        Application app = engine.getApplication(applicationId);
        if (app == null) return;

        DataObject conns = listConnections(applicationId, null, "id", ctx);
        if (conns == null) return;

        JSONArray arr = conns.getJSONArray();
        if (arr==null || arr.size()==0) return;

        for (Object conn : arr) {
            String id = ((JSONObject) conn).getString("id");
            _stopConnection(id, app, ctx);
        }
    }

    /**
     * Remove all connections for the application and/or the client.
     */
    public static void removeConnections(String applicationId, String clientId, Context ctx) throws Exception {
        if (applicationId==null && clientId==null) return;

        DataObject conns = listConnections(applicationId, clientId, "id,applicationId", ctx);
        if (conns == null) return;

        JSONArray arr = conns.getJSONArray();
        if (arr==null || arr.size()==0) return;

        for (Object conn : arr) {
            String id = ((JSONObject) conn).getString("id");
            String appId = ((JSONObject) conn).getString("applicationId");

            _removeConnection(id, appId, ctx);
        }
    }

}
