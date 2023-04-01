package cloud.phusion.express.service;

import cloud.phusion.Context;
import cloud.phusion.DataObject;
import cloud.phusion.express.ExpressService;
import cloud.phusion.storage.FileStorage;
import com.alibaba.fastjson2.JSONObject;

import java.nio.charset.StandardCharsets;

public class SchemaService {
    private static final String ROOT_PATH = "/schema";

    public static boolean hasSchema(String id, Context ctx) throws Exception {
        FileStorage storage = ctx.getEngine().getFileStorageForApplication(ExpressService.STORAGE_ID);
        String path = ROOT_PATH+"/"+id+".json";
        return storage.doesFileExist(path, ctx);
    }

    public static DataObject fetchSchema(String id, Context ctx) throws Exception {
        FileStorage storage = ctx.getEngine().getFileStorageForApplication(ExpressService.STORAGE_ID);
        String path = ROOT_PATH+"/"+id+".json";

        if (storage.doesFileExist(path, ctx)) {
            byte[] content = storage.readAllFromFile(path, ctx);
            if (content == null || content.length == 0) return null;
            else return new DataObject(new String(content, "UTF-8"));
        }
        else return null;
    }

    public static void saveSchema(String id, DataObject schema, Context ctx) throws Exception {
        String strSchema = schema.getString();
        if (strSchema==null || strSchema.length()==0) return;

        String path = ROOT_PATH+"/"+id+".json";
        FileStorage storage = ctx.getEngine().getFileStorageForApplication(ExpressService.STORAGE_ID);

        storage.saveToFile(path, strSchema.getBytes(StandardCharsets.UTF_8), ctx);
    }

    public static void removeSchema(String id, Context ctx) throws Exception {
        FileStorage storage = ctx.getEngine().getFileStorageForApplication(ExpressService.STORAGE_ID);
        String path = ROOT_PATH+"/"+id+".json";
        storage.removeFile(path, ctx);
    }

    public static void removeSchemas(String heading, Context ctx) throws Exception {
        FileStorage storage = ctx.getEngine().getFileStorageForApplication(ExpressService.STORAGE_ID);
        String[] schemas = storage.listFiles(ROOT_PATH);

        heading += ".";
        String path = ROOT_PATH+"/";

        for (String filename : schemas) {
            if (filename.startsWith(heading)) storage.removeFile(path+filename, ctx);
        }
    }

    public static void fetchSchemasIntoObject(String heading, JSONObject obj, String[] fields, Context ctx) throws Exception {
        DataObject schema;
        String schemaPrefix = heading + ".";

        for (String field : fields) {
            schema = fetchSchema(schemaPrefix + field, ctx);
            if (schema != null) obj.put(field, schema.getJSONObject());
        }
    }

    public static void saveSchemasFromObject(String heading, JSONObject obj, String[] fields, Context ctx) throws Exception {
        JSONObject schema;
        String schemaPrefix = heading + ".";

        for (String field : fields) {
            if (obj.containsKey(field)) {
                schema = obj.getJSONObject(field);
                if (schema != null) saveSchema(schemaPrefix + field, new DataObject(schema), ctx);
                else removeSchema(schemaPrefix + field, ctx);
                obj.remove(field);
            }
        }
    }

}
