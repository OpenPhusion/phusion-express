package cloud.phusion.express.service;

import cloud.phusion.*;
import cloud.phusion.application.Application;
import cloud.phusion.express.ExpressService;
import cloud.phusion.express.util.CommonCode;
import cloud.phusion.storage.DBStorage;
import cloud.phusion.storage.FileStorage;
import cloud.phusion.storage.Record;
import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONArray;
import com.alibaba.fastjson2.JSONObject;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class ClientService {
    private static final String _position = ClientService.class.getName();

    private static final String ROOT_PATH = "/client/";
    private static final String CLIENT_TABLE = "client";

    public static void init(boolean prepareTables, Context ctx) throws Exception {
        if (prepareTables) prepareDBTables(ctx);
    }

    public static void prepareDBTables(Context ctx) throws Exception {
        DBStorage storage = ctx.getEngine().getDBStorageForApplication(ExpressService.STORAGE_ID);

        storage.prepareTable(
                CLIENT_TABLE,
                "{" +
                        "    \"fields\": {" +
                        "        \"id\": \"String[50]\"," +
                        "        \"title\": \"String[50]\"," +
                        "        \"icon\": \"String[200]\"," +
                        "        \"desc\": \"String[5000]\"," +
                        "        \"createTime\": \"String[20]\"," +
                        "        \"updateTime\": \"String[20]\"" +
                        "    }," +
                        "    \"indexes\": [" +
                        "        {\"field\": \"id\",\"primary\": true}," +
                        "        {\"field\": \"updateTime\"}" +
                        "    ]" +
                        "}",
                ctx
        );
    }

    public static boolean hasClient(String id, Context ctx) throws Exception {
        FileStorage storage = ctx.getEngine().getFileStorageForApplication(ExpressService.STORAGE_ID);
        String path = ROOT_PATH+id+".json";

        return storage.doesFileExist(path, ctx);
    }

    public static DataObject listCategories(Context ctx) throws Exception {
        return TagService.listTags(TagService.TagType_Client_Category, ctx);
    }

    public static DataObject queryClients(Map<String,Object> query, String fields, long from, long length, Context ctx) throws Exception {
        List<Object> params = new ArrayList<>();
        String fromWhereClause = _composeFromWhereClause(query, params);

        return _queryClients(fromWhereClause, params, fields, from, length, ctx);
    }

    public static long countClients(Map<String,Object> query, Context ctx) throws Exception {
        List<Object> params = new ArrayList<>();
        String fromWhereClause = _composeFromWhereClause(query, params);
        String sql = "SELECT count(1) AS c " + fromWhereClause;

        DBStorage storage = ctx.getEngine().getDBStorageForApplication(ExpressService.STORAGE_ID);
        Record[] records = storage.freeQuery(sql, params, 0, 1, ctx);

        if (records==null || records.length==0) return 0l;
        else return records[0].getLong("c");
    }

    private static String _composeFromWhereClause(Map<String,Object> query, List<Object> params) {
        String search = query==null ? null : (String) query.get("search");
        String category = query==null ? null : (String) query.get("category");

        StringBuilder sql = new StringBuilder();

        sql.append("FROM ").append(ExpressService.STORAGE_ID).append('_').append(CLIENT_TABLE);

        if (category != null)
            sql.append(" C JOIN ")
                    .append(ExpressService.STORAGE_ID).append('_').append(TagService.TAG_TABLE)
                    .append(" T ON C.id=T.objId AND T.tagType='")
                    .append(TagService.TagType_Client_Category)
                    .append("'");

        if (search!=null || category!=null) sql.append(" WHERE ");

        boolean[] hasWhere = new boolean[]{false};
        CommonCode.composeWhereClause(hasWhere, "title", search, "like", sql, params);
        CommonCode.composeWhereClause(hasWhere, "T.tag", category, sql, params);

        return sql.length()==0 ? null : sql.toString();
    }

    public static DataObject listClientsById(String ids, String fields, Context ctx) throws Exception {
        if (ids==null || ids.length()==0) return null;

        StringBuilder fromWhereClause = new StringBuilder();
        fromWhereClause.append("FROM ")
                .append(ExpressService.STORAGE_ID).append('_').append(CLIENT_TABLE)
                .append(" WHERE ");

        List<Object> params = new ArrayList<>();
        CommonCode.composeWhereClauseFromStrings("id",ids,fromWhereClause,params);

        return _queryClients(fromWhereClause.toString(), params, fields, 0, 1000, ctx);
    }

    private static DataObject _queryClients(String fromWhereClause, List<Object> params, String fields, long from, long length, Context ctx) throws Exception {
        Set<String> fieldSet = CommonCode.convertStringToSet(fields, "id", "id,title,icon,desc,createTime,updateTime");

        DBStorage storage = ctx.getEngine().getDBStorageForApplication(ExpressService.STORAGE_ID);
        JSONArray result = new JSONArray();

        String selectClause = CommonCode.convertFieldSetToSelectClause(fieldSet);
        String orderClause = fromWhereClause.length()>0 ? " " : "";
        orderClause += "ORDER BY updateTime desc";

        String sql = "SELECT "+selectClause+" "+fromWhereClause+orderClause;

        Record[] list = storage.freeQuery(sql, params, from, length, ctx);

        if (list!=null && list.length>0) {
            for (Record record : list) {
                JSONObject item = new JSONObject();
                CommonCode.copyRecordFields(record, item, fieldSet);
                result.add(item);
            }
        }

        return new DataObject(result);
    }

    public static DataObject fetchClient(String id, Context ctx) throws Exception {
        FileStorage storage = ctx.getEngine().getFileStorageForApplication(ExpressService.STORAGE_ID);
        String path = ROOT_PATH+id+".json";

        if (storage.doesFileExist(path, ctx)) {
            byte[] content = storage.readAllFromFile(path, ctx);
            if (content == null || content.length == 0) return null;
            else return new DataObject(new String(content, StandardCharsets.UTF_8));
        }
        else return null;
    }

    public static void saveClient(String id, DataObject client, boolean categoryChanged, String tableSchemaChanged, Context ctx) throws Exception {
        Engine engine = ctx.getEngine();

        // Save to file

        if (client == null) return;
        JSONObject clientObj = client.getJSONObject();

        String updateTime = CommonCode.convertDatetimeString();
        String createTime = clientObj.getString("createTime");
        if (createTime==null || createTime.length()==0) createTime = updateTime;
        clientObj.put("createTime", createTime);
        clientObj.put("updateTime", updateTime);

        FileStorage fStorage = engine.getFileStorageForApplication(ExpressService.STORAGE_ID);
        String path = ROOT_PATH+id+".json";

        String strClient = clientObj.toJSONString();
        fStorage.saveToFile(path, strClient.getBytes(StandardCharsets.UTF_8), ctx);

        // Save to DB table "client"

        String[] fields = new String[]{"id","title","icon","desc","createTime","updateTime"};
        Record record = CommonCode.convertJSONObjectToRecord(clientObj, fields);
        if (record == null) return;

        DBStorage dbStorage = engine.getDBStorageForApplication(ExpressService.STORAGE_ID);
        dbStorage.upsertRecordById(CLIENT_TABLE, "id", record, ctx);

        // Save to DB table "tag"

        if (categoryChanged) {
            JSONArray arr = clientObj.getJSONArray("categories");
            TagService.saveTags(id, TagService.TagType_Client_Category, new DataObject(arr), ctx);
        }

        // Apply the changed table schema

        if (tableSchemaChanged != null) {
            JSONArray tables = clientObj.getJSONArray("tables");
            DBStorage storage = engine.getDBStorageForClient(id);
            CommonCode.prepareTables(tables, tableSchemaChanged, storage, ctx);
        }
    }

    public static void removeClient(String id, Context ctx) throws Exception {
        Engine engine = ctx.getEngine();

        // Remove connections
        ConnectionService.removeConnections(null, id, ctx);

        // Remove the client file

        FileStorage fStorage = engine.getFileStorageForApplication(ExpressService.STORAGE_ID);
        String path = ROOT_PATH+id+".json";

        fStorage.removeFile(path, ctx);

        // Remove from DB

        DBStorage dbStorage = engine.getDBStorageForApplication(ExpressService.STORAGE_ID);
        dbStorage.deleteRecordById(CLIENT_TABLE, "id", id, ctx);

        TagService.removeTags(id, ctx);
    }

}
