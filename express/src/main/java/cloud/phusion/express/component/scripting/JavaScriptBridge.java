package cloud.phusion.express.component.scripting;

import cloud.phusion.Context;
import cloud.phusion.Engine;
import cloud.phusion.PhusionException;
import cloud.phusion.storage.DBStorage;
import cloud.phusion.storage.FileStorage;
import cloud.phusion.storage.KVStorage;
import cloud.phusion.storage.Record;
import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;
import com.eclipsesource.v8.*;

import java.io.FileOutputStream;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * JavaScript - Java Bridge.
 */
public class JavaScriptBridge implements JavaCallback {
    private static final String _position = JavaScriptBridge.class.getName();

    private Map<String,Object> map;

    public JavaScriptBridge(Map<String,Object> localMap) {
        this.map = localMap;
    }

    /**
     * From JavaScript to call Java.
     */
    public final static String JavaCallback_Func = "_callJava";
    public final static String JavaCallback_AsyncResult = "_callback";

    /**
     * From Java to call JavaScript.
     *
     * Both input and output are the Transaction object (JSON string)
     */
    public final static String JavascriptTransaction_Func = "_runTransaction";

    public final static String Javascript_Func = "_run";

    /**
     * Params: type, instance, method, arg1, arg2, ...
     *
     * Type: "context"
     * - instance: null
     * - method(arg1, arg2, ...):
     *      logInfo(msg),
     *      logError(msg)
     *
     * Type: "kvstorage"
     * - instance: "application|integration|client id"
     * - method(arg1, arg2, ...):
     *      put(key,value,ms=empty),
     *      get(key),
     *      doesExist(key),
     *      remove(key),
     *      lock(key,ms=empty),
     *      unlock(key)
     *
     * Type: "filestorage"
     * - instance: "application|integration|client id"
     * - method(arg1, arg2, ...):
     *      doesFileExist(path),
     *      doesPublicFileExist(path),
     *      getFileSize(path),
     *      getPublicFileSize(path),
     *      listFiles(path),
     *      listPublicFiles(path),
     *      listFolders(path),
     *      listPublicFolders(path),
     *      saveToFile(path,content),
     *      saveToPublicFile(path,content),
     *      readFromFile(path, inString=false),
     *      readFromPublicFile(path, inString=false),
     *      removeFile(path),
     *      removePublicFile(path),
     *      removeAll(),
     *      getPublicFileUrl(path)
     *
     * Type: "dbstorage"
     * - instance: "application|integration|client id"
     * - method(arg1, arg2, ...):
     *      doesTableExist(tableName),
     *      insertRecord(tableName,record),
     *      queryRecords(tableName,selectClause,whereClause,params,orderClause,from=empty,length=empty),
     *      queryCount(tableName,selectClause,whereClause,params),
     *      queryRecordById(tableName,selectClause,idField,params),
     *      updateRecords(tableName,record,whereClause,params),
     *      updateRecordById(tableName,record,idField,params),
     *      replaceRecordById(tableName,record,idField,params),
     *      deleteRecords(tableName,whereClause,params),
     *      deleteRecordById(tableName,idField,params)
     *
     *      Note: params = {"0":..., "1":...., "2":....}
     */
    @Override
    public Object invoke(V8Object receiver, V8Array params) {
        if (params.length() > 0) {
            String type = params.getString(0);
            String instance = params.getString(1);
            String method = params.getString(2);
            String[] args = new String[params.length()-3];
            Object extraParam = null;

            if (type.equals("filestorage") && (
                    method.equals("saveToFile") || method.equals("saveToPublicFile")
                    )) {
                for (int i = 3; i < params.length()-1; i++) {
                    args[i - 3] = params.getString(i);
                }
                extraParam = params.get(params.length()-1);
            }
            else {
                for (int i = 3; i < params.length(); i++) {
                    args[i - 3] = params.getString(i);
                }
            }

            switch (type) {
                case "context":
                    return _execContextMethods(method, args);
                case "kvstorage":
                    return _execKVStorageMethods(instance, method, args);
                case "filestorage":
                    return _execFileStorageMethods(instance, method, args, extraParam);
                case "dbstorage":
                    return _execDBStorageMethods(instance, method, args);
                default:
                    return null;
            }
        }
        else {
            // For Ping
            return "Java bridge @" + Thread.currentThread().getName();
        }
    }

    private Object _execContextMethods(String method, String[] args) {
        Context c = (Context) map.get("c");

        switch (method) {
            case "logInfo":
                if (args!=null && args.length==3) c.logInfo(args[0], args[1], args[2]);
                return null;
            case "logError":
                if (args!=null && args.length==3) c.logError(args[0], args[1], args[2]);
                return null;
            default:
                return null;
        }
    }

    private Object _execKVStorageMethods(String instance, String method, String[] args) {
        Context ctx = (Context) map.get("c");
        Engine engine = ctx.getEngine();

        int pos = instance.indexOf(' ');
        String type = instance.substring(0,pos);
        String id = instance.substring(pos+1);
        KVStorage storage = null;

        try {
            switch (type) {
                case "application":
                    storage = engine.getKVStorageForApplication(id);
                    break;
                case "integration":
                    storage = engine.getKVStorageForIntegration(id);
                    break;
                case "client":
                    storage = engine.getKVStorageForClient(id);
                    break;
            }

            if (storage != null) {
                switch (method) {
                    case "put":
                        if (args!=null && args.length==3) {
                            String ms = args[2];
                            if (ms==null || ms.length()==0) storage.put(args[0], args[1], ctx);
                            else storage.put(args[0], args[1], Integer.parseInt(ms), ctx);
                        }
                        return null;
                    case "get":
                        if (args!=null && args.length==1) {
                            Object result = storage.get(args[0], ctx);
                            return result instanceof String ? result : result.toString();
                        }
                        return null;
                    case "doesExist":
                        if (args!=null && args.length==1) {
                            boolean result = storage.doesExist(args[0], ctx);
                            return result ? "true" : "false";
                        }
                        return null;
                    case "remove":
                        if (args!=null && args.length==1) {
                            storage.remove(args[0], ctx);
                        }
                        return null;
                    case "lock":
                        if (args!=null && args.length==2) {
                            String ms = args[1];
                            boolean result;

                            if (ms==null || ms.length()==0) result = storage.lock(args[0], ctx);
                            else result = storage.lock(args[0], Integer.parseInt(ms), ctx);

                            return result ? "true" : "false";
                        }
                        return null;
                    case "unlock":
                        if (args!=null && args.length==1) {
                            storage.unlock(args[0], ctx);
                        }
                        return null;
                    default:
                        return null;
                }
            }
            else
                return null;
        } catch (Exception ex) {
            ctx.logError(
                    _position, "Failed to execute KVStorage method",
                    String.format("instance=%s, method=%s, args=%s", instance, method, Arrays.toString(args)),
                    ex);
            return null;
        }
    }

    private Object _execFileStorageMethods(String instance, String method, String[] args, Object extraParam) {
        Context ctx = (Context) map.get("c");
        Engine engine = ctx.getEngine();

        int pos = instance.indexOf(' ');
        String type = instance.substring(0,pos);
        String id = instance.substring(pos+1);
        FileStorage storage = null;

        try {
            switch (type) {
                case "application":
                    storage = engine.getFileStorageForApplication(id);
                    break;
                case "integration":
                    storage = engine.getFileStorageForIntegration(id);
                    break;
                case "client":
                    storage = engine.getFileStorageForClient(id);
                    break;
            }

            if (storage != null) {
                switch (method) {
                    case "doesFileExist":
                        if (args!=null && args.length==1) {
                            boolean result;
                            result = storage.doesFileExist(args[0], ctx);
                            return result ? "true" : "false";
                        }
                        return null;
                    case "doesPublicFileExist":
                        if (args!=null && args.length==1) {
                            boolean result;
                            result = storage.doesPublicFileExist(args[0], ctx);
                            return result ? "true" : "false";
                        }
                        return null;
                    case "getFileSize":
                        if (args!=null && args.length==1) {
                            long result = storage.getFileSize(args[0], ctx);
                            return ""+result;
                        }
                        return null;
                    case "getPublicFileSize":
                        if (args!=null && args.length==1) {
                            long result = storage.getPublicFileSize(args[0], ctx);
                            return ""+result;
                        }
                        return null;
                    case "listFolders":
                        if (args!=null && args.length==1) {
                            String[] result = storage.listFolders(args[0], ctx);
                            return _stringArrayToJSONString(result);
                        }
                        return null;
                    case "listPublicFolders":
                        if (args!=null && args.length==1) {
                            String[] result = storage.listPublicFolders(args[0], ctx);
                            return _stringArrayToJSONString(result);
                        }
                        return null;
                    case "listFiles":
                        if (args!=null && args.length==1) {
                            String[] result = storage.listFiles(args[0], ctx);
                            return _stringArrayToJSONString(result);
                        }
                        return null;
                    case "listPublicFiles":
                        if (args!=null && args.length==1) {
                            String[] result = storage.listPublicFiles(args[0], ctx);
                            return _stringArrayToJSONString(result);
                        }
                        return null;
                    case "saveToFile":
                        if (args!=null && args.length==2) {
                            if (extraParam instanceof String) {
                                storage.saveToFile(args[0], ((String) extraParam).getBytes(StandardCharsets.UTF_8), ctx);
                            }
                            else if (extraParam instanceof V8ArrayBuffer) _saveToFileByBytes(storage, args[0], (V8ArrayBuffer) extraParam, ctx, true);
                        }
                        return null;
                    case "saveToPublicFile":
                        if (args!=null && args.length==2) {
                            if (extraParam instanceof String) {
                                storage.saveToPublicFile(args[0], ((String) extraParam).getBytes(StandardCharsets.UTF_8), ctx);
                            }
                            else if (extraParam instanceof V8ArrayBuffer) _saveToFileByBytes(storage, args[0], (V8ArrayBuffer) extraParam, ctx, false);
                        }
                        return null;
                    case "readFromFile":
                        if (args!=null && args.length==2) {
                            String inString = args[1];
                            boolean bInString = (inString==null || inString.length()==0) ? false : Boolean.parseBoolean(inString);
                            NodeJS node = (NodeJS) map.get("n");
                            if (bInString) {
                                byte[] result = storage.readAllFromFile(args[0], ctx);
                                return new String(result, StandardCharsets.UTF_8);
                            }
                            else return _readFromFileByBytes(storage, args[0], ctx, node, true);
                        }
                        return null;
                    case "readFromPublicFile":
                        if (args!=null && args.length==2) {
                            String inString = args[1];
                            boolean bInString = (inString==null || inString.length()==0) ? false : Boolean.parseBoolean(inString);
                            NodeJS node = (NodeJS) map.get("n");
                            if (bInString) {
                                byte[] result = storage.readAllFromPublicFile(args[0], ctx);
                                return new String(result, StandardCharsets.UTF_8);
                            }
                            else return _readFromFileByBytes(storage, args[0], ctx, node, false);
                        }
                        return null;
                    case "removeFile":
                        if (args!=null && args.length==1) {
                            storage.removeFile(args[0], ctx);
                        }
                        return null;
                    case "removePublicFile":
                        if (args!=null && args.length==1) {
                            storage.removePublicFile(args[0], ctx);
                        }
                        return null;
                    case "removeAll":
                        if (args!=null && args.length==0) {
                            storage.removeAll(ctx);
                        }
                        return null;
                    case "getPublicFileUrl":
                        if (args!=null && args.length==1) {
                            return storage.getPublicFileUrl(args[0]);
                        }
                        return null;
                    default:
                        return null;
                }
            }
            else
                return null;
        } catch (Exception ex) {
            ctx.logError(
                    _position, "Failed to execute FileStorage method",
                    String.format("instance=%s, method=%s, args=%s", instance, method, Arrays.toString(args)),
                    ex);
            return null;
        }
    }

    private String _stringArrayToJSONString(String[] arr) {
        if (arr==null || arr.length==0) return null;

        StringBuilder result = new StringBuilder();
        result.append("[");
        for (int i = 0; i < arr.length; i++) {
            if (i > 0) result.append(",");
            result.append("\"").append(arr[i]).append("\"");
        }
        result.append("]");

        return result.toString();
    }

    private Object _execDBStorageMethods(String instance, String method, String[] args) {
        Context ctx = (Context) map.get("c");
        Engine engine = ctx.getEngine();

        int pos = instance.indexOf(' ');
        String type = instance.substring(0,pos);
        String id = instance.substring(pos+1);
        DBStorage storage = null;

        try {
            switch (type) {
                case "application":
                    storage = engine.getDBStorageForApplication(id);
                    break;
                case "integration":
                    storage = engine.getDBStorageForIntegration(id);
                    break;
                case "client":
                    storage = engine.getDBStorageForClient(id);
                    break;
            }

            if (storage != null) {
                switch (method) {
                    case "doesTableExist":
                        if (args!=null && args.length==1) {
                            boolean result = storage.doesTableExist(args[0], ctx);
                            return result ? "true" : "false";
                        }
                        return null;
                    case "insertRecord":
                        if (args!=null && args.length==2) {
                            int result = storage.insertRecord(args[0],new Record(args[1]), ctx);
                            return ""+result;
                        }
                        return null;
                    case "queryRecords":
                        if (args!=null && args.length==9) {
                            List<Object> params = _paramsJSONToList(args[5]);
                            long from = (args[7]!=null && args[7].length()>0) ? Long.parseLong(args[7]) : 0;
                            long length = (args[8]!=null && args[8].length()>0) ? Long.parseLong(args[8]) : 0;
                            if (length == 0) length = 100;

                            Record[] result = storage.queryRecords(args[0],args[1],args[2],args[3],args[4],params,args[6],from,length, ctx);
                            return _recordArrToJSON(result);
                        }
                        return null;
                    case "queryCount":
                        if (args!=null && args.length==4) {
                            List<Object> params = _paramsJSONToList(args[3]);
                            long result = storage.queryCount(args[0],args[1],args[2],params, ctx);
                            return ""+result;
                        }
                        return null;
                    case "queryRecordById":
                        if (args!=null && args.length==4) {
                            List<Object> params = _paramsJSONToList(args[3]);
                            Record result = storage.queryRecordById(args[0],args[1],args[2],params.get(0), ctx);
                            return result==null ? "" : result.toJSONString();
                        }
                        return null;
                    case "updateRecords":
                        if (args!=null && args.length==4) {
                            List<Object> params = _paramsJSONToList(args[3]);
                            int result = storage.updateRecords(args[0],new Record(args[1]),args[2],params, ctx);
                            return ""+result;
                        }
                        return null;
                    case "updateRecordById":
                        if (args!=null && args.length==4) {
                            List<Object> params = _paramsJSONToList(args[3]);
                            int result = storage.updateRecordById(args[0],new Record(args[1]),args[2],params.get(0), ctx);
                            return ""+result;
                        }
                        return null;
                    case "replaceRecordById":
                        if (args!=null && args.length==4) {
                            List<Object> params = _paramsJSONToList(args[3]);
                            int result = storage.replaceRecordById(args[0],new Record(args[1]),args[2],params.get(0), ctx);
                            return ""+result;
                        }
                        return null;
                    case "deleteRecords":
                        if (args!=null && args.length==3) {
                            List<Object> params = _paramsJSONToList(args[2]);
                            int result = storage.deleteRecords(args[0],args[1],params, ctx);
                            return ""+result;
                        }
                        return null;
                    case "deleteRecordById":
                        if (args!=null && args.length==3) {
                            List<Object> params = _paramsJSONToList(args[2]);
                            int result = storage.deleteRecordById(args[0],args[1],params.get(0), ctx);
                            return ""+result;
                        }
                        return null;
                    default:
                        return null;
                }
            }
            else
                return null;
        } catch (Exception ex) {
            ctx.logError(
                    _position, "Failed to execute DBStorage method",
                    String.format("instance=%s, method=%s, args=%s", instance, method, Arrays.toString(args)),
                    ex);
            return null;
        }
    }

    private String _recordArrToJSON(Record[] records) {
        StringBuilder result = new StringBuilder();
        result.append("[");

        if (records!=null && records.length>0) {
            for (int i = 0; i <records.length; i++) {
                if (i > 0) result.append(",");
                result.append(records[i].toJSONString());
            }
        }

        result.append("]");
        return result.toString();
    }

    /**
     * @param json {"0":..., "1":...., "2":....}
     */
    private List<Object> _paramsJSONToList(String json) {
        ArrayList<Object> arr = new ArrayList<>();

        if (json != null) {
            JSONObject obj = JSON.parseObject(json);
            int length = obj.size();

            for (int i = 0; i < length; i++) {
                arr.add(obj.get("" + i));
            }
        }

        return arr;
    }

    private void _saveToFileByBytes(FileStorage storage, String path, V8ArrayBuffer content, Context ctx, boolean isPrivate) throws Exception {
        try (ByteBufferBackedInputStream in = new ByteBufferBackedInputStream(content.getBackingStore())) {
            if (isPrivate) storage.saveToFile(path, in, ctx);
            else storage.saveToFile(path, in, ctx);
        }

        content.release();
    }

    private static final int LOCAL_BUFFER_LENGTH = 1024;

    private V8ArrayBuffer _readFromFileByBytes(FileStorage storage, String path, Context ctx, NodeJS node, boolean isPrivate) throws Exception {
        int size = (int)(isPrivate ? storage.getFileSize(path, ctx) : storage.getPublicFileSize(path, ctx));

        V8ArrayBuffer result = new V8ArrayBuffer(node.getRuntime(), size);
        ByteBuffer resultBuf = result.getBackingStore();

        try (InputStream in = isPrivate ? storage.readFromFile(path, ctx) : storage.readFromPublicFile(path, ctx)) {
            int offset = 0;
            byte[] buf = new byte[LOCAL_BUFFER_LENGTH];
            while (offset < size) {
                int bytes = in.read(buf, 0, LOCAL_BUFFER_LENGTH);
                if (bytes == -1) break;
                resultBuf.put(buf, 0, bytes);
                offset += bytes;
            }
        }

        return result;
    }

}
