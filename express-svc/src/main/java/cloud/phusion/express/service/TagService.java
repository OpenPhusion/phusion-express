package cloud.phusion.express.service;

import cloud.phusion.Context;
import cloud.phusion.DataObject;
import cloud.phusion.Engine;
import cloud.phusion.express.ExpressService;
import cloud.phusion.express.util.CommonCode;
import cloud.phusion.storage.DBStorage;
import cloud.phusion.storage.FileStorage;
import cloud.phusion.storage.Record;
import com.alibaba.fastjson2.JSONArray;
import com.alibaba.fastjson2.JSONObject;

import java.nio.charset.StandardCharsets;
import java.util.*;

/**
 * For tag-like information, such as application's category and protocol
 */
public class TagService {
    private static final String _position = TagService.class.getName();

    public static final String TAG_TABLE = "tag";

    public static final String TagType_App_Category = "applicationCategory";
    public static final String TagType_App_Protocol = "applicationProtocol";
    public static final String TagType_It_Category = "integrationCategory";
    public static final String TagType_Client_Category = "clientCategory";

    public static void init(boolean prepareTables, Context ctx) throws Exception {
        if (prepareTables) prepareDBTables(ctx);
    }

    public static void prepareDBTables(Context ctx) throws Exception {
        DBStorage storage = ctx.getEngine().getDBStorageForApplication(ExpressService.STORAGE_ID);

        storage.prepareTable(
                TAG_TABLE,
                "{" +
                        "    \"fields\": {" +
                        "        \"objId\": \"String[50]\"," +
                        "        \"tagType\": \"String[20]\"," +
                        "        \"tag\": \"String[50]\"" +
                        "    }," +
                        "    \"indexes\": [" +
                        "        {\"fields\": [\"objId\",\"tagType\"]}," +
                        "        {\"fields\": [\"tag\",\"tagType\"]}," +
                        "        {\"field\": \"objId\"}" +
                        "    ]" +
                        "}",
                ctx
        );
    }
    public static DataObject listTags(String type, Context ctx) throws Exception {
        DBStorage storage = ctx.getEngine().getDBStorageForApplication(ExpressService.STORAGE_ID);
        JSONObject result = new JSONObject();
        if (type!=null && type.length()==0) type = null;

        String select = "tag, count(1) as c";
        String where = type==null ? "" : "tagType=?";
        String group = "tag";
        List<Object> params = new ArrayList<>();
        if (type != null) params.add(type);

        Record[] list = storage.queryRecords(TAG_TABLE, select, where, group, null, params, null, 0, 1000, ctx);

        if (list!=null && list.length>0) {
            for (Record record : list) {
                String tag = record.getString("tag");
                if (tag == null) tag = "";
                result.put(tag, record.getInteger("c"));
            }
        }

        return new DataObject(result);
    }

    // Return JSON array of Strings
    public static DataObject getTags(String objId, String type, Context ctx) throws Exception {
        DBStorage storage = ctx.getEngine().getDBStorageForApplication(ExpressService.STORAGE_ID);
        JSONArray result = new JSONArray();

        String select = "tag";
        String where = "objId=? AND tagType=?";
        List<Object> params = new ArrayList<>();
        params.add(objId);
        params.add(type);

        Record[] list = storage.queryRecords(TAG_TABLE, select, where, params, null, 0, 100, ctx);

        if (list!=null && list.length>0) {
            for (Record record : list) result.add(record.getString("tag"));
        }

        return new DataObject(result);
    }

    // tags: JSON array of Strings
    public static void saveTags(String objId, String type, DataObject tags, Context ctx) throws Exception {
        // Not in DB transaction, to be optimized.

        removeTags(objId, type, ctx); // Remove the previous tags

        if (tags == null) return;

        JSONArray arr = tags.getJSONArray();

        if (arr.size() == 0) return;

        DBStorage storage = ctx.getEngine().getDBStorageForApplication(ExpressService.STORAGE_ID);
        List<Object> params = new ArrayList<>();
        Set<String> tagSet = new HashSet<>();

        for (int i = 0; i < arr.size(); i++) {
            String tag = arr.getString(i);
            if (tagSet.contains(tag)) continue;

            params.add(objId);
            params.add(type);
            params.add(tag);

            tagSet.add(tag);
        }

        storage.insertRecords(TAG_TABLE, "objId,tagType,tag", params, ctx);
    }

    public static void removeTags(String objId, Context ctx) throws Exception {
        removeTags(objId, null, ctx);
    }

    public static void removeTags(String objId, String type, Context ctx) throws Exception {
        DBStorage storage = ctx.getEngine().getDBStorageForApplication(ExpressService.STORAGE_ID);

        String where = null;
        List<Object> params = new ArrayList<>();

        if (type == null) {
            where = "objId=?";
            params.add(objId);
        }
        else {
            where = "objId=? AND tagType=?";
            params.add(objId);
            params.add(type);
        }

        storage.deleteRecords(TAG_TABLE, where, params, ctx);
    }

}
