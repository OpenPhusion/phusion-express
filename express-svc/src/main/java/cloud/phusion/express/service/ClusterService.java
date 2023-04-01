package cloud.phusion.express.service;

import cloud.phusion.Context;
import cloud.phusion.DataObject;
import cloud.phusion.Engine;
import cloud.phusion.ScheduledTask;
import cloud.phusion.express.ExpressService;
import cloud.phusion.express.component.storage.KVStorageImpl;
import cloud.phusion.express.util.CommonCode;
import cloud.phusion.storage.DBStorage;
import cloud.phusion.storage.Record;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Heartbeat: every "heartbeatInterval" seconds (defined in application.properties "cluster.heartbeat.intervalSeconds"),
 * update the "CLUSTER_TABLE". If an engine's heartbeat has been lost for longer than "heartbeatInterval" * 1.5 seconds,
 * any object on this engine will be thought as not running.
 */
public class ClusterService {
    private static final String CLUSTER_TABLE = "cluster_status";
    private static final String MSG_CHANNEL = "message";

    public static final String HEARTBEAT_INTERVAL = "cluster.heartbeat.intervalSeconds";
    public static final String SVC_ADDR = "cluster.serviceAddress";
    public static final String STATUS_HEARTBEAT = "heartbeat";

    public static final String OBJECT_ENGINE = "engine";
    public static final String OBJECT_APPLICATION = "application";
    public static final String OBJECT_INTEGRATION = "integration";
    public static final String OBJECT_CONNECTION = "connection";
    public static final String OBJECT_USER = "user";

    private static long heartbeatInterval = 10;
    private static long validStatusInterval = 15;
    private static String addr = null;

    public static void init(String hearbeatIntervalStr, String svcAddress, boolean prepareTables, Context ctx) throws Exception {
        heartbeatInterval = Long.parseLong(hearbeatIntervalStr);
        validStatusInterval = Math.round(heartbeatInterval * 1.5);
        addr = svcAddress;
        if (addr!=null && addr.length()==0) addr = null;

        if (prepareTables) prepareDBTables(ctx);

        Engine engine = ctx.getEngine();

        // Clear all existing status
        DBStorage dbStorage = engine.getDBStorageForApplication(ExpressService.STORAGE_ID);
        List<Object> params = new ArrayList<>();
        params.add(engine.getId());
        dbStorage.deleteRecords(CLUSTER_TABLE, "engineId=?", params, ctx);

        // Heartbeat task
        engine.scheduleTask(
                ExpressService.STORAGE_ID + "-heartbeat",
                new ClusterServiceHandler(null, null), null, heartbeatInterval, 0,
                false, ctx
        );

        // Message channel

        KVStorageImpl kvStorage = (KVStorageImpl) engine.getKVStorageForApplication(ExpressService.STORAGE_ID);
        kvStorage.subscribe(MSG_CHANNEL, new ClusterServiceHandler(MSG_CHANNEL, ctx), ctx);
    }

    public static void prepareDBTables(Context ctx) throws Exception {
        DBStorage storage = ctx.getEngine().getDBStorageForApplication(ExpressService.STORAGE_ID);

        storage.prepareTable(
                CLUSTER_TABLE,
                "{" +
                        "    \"fields\": {" +
                        "        \"objType\": \"String[20]\"," +
                        "        \"objId\": \"String[50]\"," +
                        "        \"objStatus\": \"String[20]\"," +
                        "        \"engineId\": \"String[10]\"," +
                        "        \"address\": \"String[100]\"," +
                        "        \"updateTime\": \"String[20]\"" +
                        "    }," +
                        "    \"indexes\": [" +
                        "        {\"fields\": [\"objType\",\"objId\",\"engineId\"], \"primary\":true}," +
                        "        {\"fields\": [\"objType\",\"objId\"]}" +
                        "    ]" +
                        "}",
                ctx
        );
    }

    public static void heartbeat(Context ctx) throws Exception {
        ClusterService.updateObjectStatus(OBJECT_ENGINE, ctx.getEngine().getId(), STATUS_HEARTBEAT, addr, ctx);
    }

    public static void updateObjectStatus(String type, String id, String status, Context ctx) throws Exception {
        updateObjectStatus(type, id, status, null, ctx);
    }

    public static void updateObjectStatus(String type, String id, String status, String svcAddress, Context ctx) throws Exception {
        Engine engine = ctx.getEngine();
        DBStorage storage = engine.getDBStorageForApplication(ExpressService.STORAGE_ID);

        Record record = new Record();
        record.setValue("objType", type);
        record.setValue("objId", id);
        record.setValue("objStatus", status);
        record.setValue("engineId", engine.getId());
        record.setValue("updateTime", CommonCode.convertDatetimeString());
        if (svcAddress != null) record.setValue("address", svcAddress);

        ArrayList<Object> params = new ArrayList<>();
        params.add(type);
        params.add(id);
        params.add(engine.getId());

        boolean noLog = type.equals(OBJECT_ENGINE) && status.equals(STATUS_HEARTBEAT); // Purge the log of heartbeat
        storage.upsertRecord(CLUSTER_TABLE, "objType=? and objId=? and engineId=?", params, record, noLog, ctx);
    }

    public static void removeObject(String type, String id, Context ctx) throws Exception {
        Engine engine = ctx.getEngine();
        DBStorage storage = engine.getDBStorageForApplication(ExpressService.STORAGE_ID);

        ArrayList<Object> params = new ArrayList<>();
        params.add(type);
        params.add(id);
        params.add(engine.getId());

        storage.deleteRecords(CLUSTER_TABLE, "objType=? AND objId=? AND engineId=?", params, ctx);
    }

    public static DataObject listEngines(Context ctx) throws Exception {
        StringBuilder result = new StringBuilder();
        DBStorage storage = ctx.getEngine().getDBStorageForApplication(ExpressService.STORAGE_ID);

        List<Object> params = new ArrayList<>();
        params.add(OBJECT_ENGINE);
        params.add(CommonCode.convertDatetimeString(-validStatusInterval));

        Record[] records = storage.queryRecords(CLUSTER_TABLE, "objId,address,updateTime",
                "objType=? AND updateTime>=?", params, ctx); // If the update time is too early, the engine is down

        result.append("{\"heartbeatExpireInterval\":")
                .append(validStatusInterval).append(",\"activeEngines\":[");

        if (records!=null && records.length>0) {
            boolean isFirst = true;
            for (Record record : records) {
                if (isFirst) isFirst = false;
                else result.append(",");

                result.append("{\"engineId\":\"").append(record.getString("objId"))
                        .append("\",\"lastHeartbeatTime\":\"").append(record.getString("updateTime"))
                        .append("\"");

                String addr = record.getString("address");
                if (addr!=null && addr.length()>0)
                    result.append(",\"address\":\"").append(addr).append("\"");

                result.append("}");
            }
        }

        result.append("]}");

        return new DataObject(result.toString());
    }

    public static DataObject listObjectStatus(String type, String id, Context ctx) throws Exception {
        DataObject engines = listEngines(ctx);
        if (engines == null) return null;

        // engineId -> lastHeartbeatTime
        Map<String,Object> engineTime = CommonCode.convertJSONArrayToMap(
                engines.getJSONObject().getJSONArray("activeEngines"), "engineId", "lastHeartbeatTime");

        StringBuilder result = new StringBuilder();
        DBStorage storage = ctx.getEngine().getDBStorageForApplication(ExpressService.STORAGE_ID);

        List<Object> params = new ArrayList<>();
        params.add(type);
        params.add(id);

        Record[] records = storage.queryRecords(CLUSTER_TABLE, "objStatus,engineId,updateTime",
                "objType=? AND objId=?", params, ctx);

        result.append("[");
        if (records!=null && records.length>0) {
            boolean isFirst = true;

            for (Record record : records) {
                String engineId = record.getString("engineId");
                if (engineTime.get(engineId) == null) continue; // The engine is down (because it is filtered off by listEngines())

                if (isFirst) isFirst = false;
                else result.append(",");
                result.append("{\"engineId\":\"").append(engineId)
                        .append("\",\"updateTime\":\"").append(record.getString("updateTime"))
                        .append("\",\"status\":\"").append(record.getString("objStatus"))
                        .append("\"}");
            }
        }
        result.append("]");

        return new DataObject(result.toString());
    }

    public static void sendMessage(String type, String action, String id, Context ctx) throws Exception {
        KVStorageImpl storage = (KVStorageImpl) ctx.getEngine().getKVStorageForApplication(ExpressService.STORAGE_ID);
        storage.publish(MSG_CHANNEL, String.join(",",type,action,id), ctx);
    }

}
