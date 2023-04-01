package cloud.phusion.express.util;

import cloud.phusion.Context;
import cloud.phusion.DataObject;
import cloud.phusion.Engine;
import cloud.phusion.EngineFactory;
import cloud.phusion.storage.DBStorage;
import cloud.phusion.storage.Record;
import com.alibaba.fastjson2.JSONArray;
import com.alibaba.fastjson2.JSONObject;

import java.lang.reflect.Array;
import java.text.SimpleDateFormat;
import java.util.*;

public class CommonCode {
    private static final String _position = CommonCode.class.getName();

    /**
     * "str": comma-separated list.
     * "mustHaveStr": comma-separated list.
     * "defaultStr": if "str" is empty, take "defaultStr".
     */
    public static Set<String> convertStringToSet(String str) {
        return convertStringToSet(str, null, "");
    }

    public static Set<String> convertStringToSet(String str, String mustHaveStr, String defaultStr) {
        Set<String> partSet = new HashSet<>();
        if (str==null || str.length()==0) str = defaultStr;

        String[] parts = str.split(",");
        for (int i = 0; i < parts.length; i++) {
            parts[i] = parts[i].trim();
        }

        partSet.addAll(Arrays.asList(parts));

        if (mustHaveStr!=null && mustHaveStr.length()>0) {
            parts = mustHaveStr.split(",");
            for (int i = 0; i < parts.length; i++) {
                parts[i] = parts[i].trim();
            }
            partSet.addAll(Arrays.asList(parts));
        }

        if (partSet.contains("")) partSet.remove("");
        return partSet;
    }

    private static Set<String> SQL_KEYWORDS = new HashSet<>();
    static {
        SQL_KEYWORDS.add("desc");
    }

    public static String convertFieldSetToSelectClause(Set<String> fields) {
        StringBuilder result = new StringBuilder();
        boolean isFirst = true;

        for (String field : fields) {
            if (isFirst) isFirst = false;
            else result.append(",");

            if (SQL_KEYWORDS.contains(field)) result.append("`").append(field).append("`");
            else result.append(field);
        }

        return result.toString();
    }

    /**
     * Note: the "fields" may be changed after calling this function.
     */
    public static void copyRecordFields(Record from, JSONObject to, Set<String> fields) {
        copyRecordFields(from, to, fields, null);
    }

    /**
     * fieldTypes: fieldName -> fieldType ("String", "Boolean")
     */
    public static void copyRecordFields(Record from, JSONObject to, Set<String> fields, Map<String,String> fieldTypes) {
        if (fields!=null && fields.size()>0) {
            for (String field : fields) {
                String type = fieldTypes==null ? null : fieldTypes.get(field);

                if (type == null) to.put(field, from.getValue(field));
                else if (type.equals("String")) to.put(field, from.getString(field));
                else if (type.equals("Boolean")) to.put(field, from.getBoolean(field));
                else to.put(field, from.getValue(field));
            }
        }
    }

    public static Record convertJSONObjectToRecord(JSONObject obj, String[] fields) {
        if (obj == null) return null;
        else {
            Record result = new Record();

            for (String field : fields) {
                result.setValue(field, obj.get(field));
            }

            return result;
        }
    }

    public static Map<String,Object> convertJSONArrayToMap(JSONArray arr, String keyField) {
        return convertJSONArrayToMap(arr, keyField, null);
    }

    public static Map<String,Object> convertJSONArrayToMap(JSONArray arr, String keyField, String valueField) {
        Map<String,Object> result = new HashMap<>();

        if (arr!=null && arr.size()>0) {
            for (int i = 0; i < arr.size(); i++) {
                JSONObject item = arr.getJSONObject(i);

                if (valueField == null)
                    result.put(item.getString(keyField), item);
                else
                    result.put(item.getString(keyField), item.get(valueField));
            }
        }

        return result;
    }

    public static JSONObject parseDataToJSONObject(DataObject obj, Context ctx) {
        try {
            return obj.getJSONObject();
        } catch (Exception ex) {
            ServiceLogger.error(_position, "Failed to parse content", "traceId="+ctx.getId(), ex);
            return null;
        }
    }

    public static JSONObject findItemInJSONArray(JSONArray arr, String itemKey, String itemValue) {
        if (arr!=null && arr.size()>0) {
            for (int i = 0; i < arr.size(); i++) {
                JSONObject item = arr.getJSONObject(i);
                if (itemValue.equals(item.getString(itemKey))) return item;
            }
        }

        return null;
    }

    public static void insertItemIntoJSONArray(JSONObject obj, String arrayKey, String itemKey, JSONObject item) {
        JSONArray arr = obj.getJSONArray(arrayKey);

        if (arr == null) {
            arr = new JSONArray();
            arr.add(item);
            obj.put(arrayKey, arr);
        }
        else {
            int pos = -1;

            for (int i = 0; i < arr.size(); i++) {
                JSONObject itemInArr = arr.getJSONObject(i);
                if (item.getString(itemKey).equals(itemInArr.getString(itemKey))) {
                    pos = i;
                    break;
                }
            }

            if (pos < 0) arr.add(item);
            else {
                arr.remove(pos);
                arr.add(pos, item);
            }
        }
    }

    public static void removeTablePhysically(String type, String id, String tableName, Context ctx) throws Exception {
        Engine engine = ctx.getEngine();
        DBStorage storage;

        switch (type) {
            case "application": storage = engine.getDBStorageForApplication(id); break;
            case "client": storage = engine.getDBStorageForClient(id); break;
            case "integration": storage = engine.getDBStorageForIntegration(id); break;
            default: storage = null;
        }

        if (storage == null) return;

        if (storage.doesTableExist(tableName, ctx))
            storage.removeTable(tableName, ctx);
    }

    public static void prepareTable(JSONObject table, DBStorage storage, Context ctx) throws Exception {
        JSONArray tables = new JSONArray();
        tables.add(table);

        prepareTables(tables, table.getString("name"), storage, ctx);
    }

    public static void prepareTables(JSONArray tables, DBStorage storage, Context ctx) throws Exception {
        prepareTables(tables, null, storage, ctx);
    }

    public static void prepareTables(JSONArray tables, String specifiedTable, DBStorage storage, Context ctx) throws Exception {
        if (tables!=null && tables.size()>0) {
            for (Object table : tables) {
                JSONObject tableObjInput = (JSONObject) table;
                String name = tableObjInput.getString("name");
                if (name==null || name.length()==0 || tableObjInput.getJSONObject("fields")==null) continue;
                if (specifiedTable!=null && ! name.equals(specifiedTable)) continue;

                JSONObject tableObj = new JSONObject();
                tableObj.put("fields", tableObjInput.get("fields"));
                tableObj.put("indexes", tableObjInput.get("indexes"));

                storage.prepareTable(name, tableObj.toJSONString(), ctx);
            }
        }
    }

    public static String convertDatetimeString() {
        return convertDatetimeString(new Date());
    }

    public static String convertDatetimeString(long secondsToShift) {
        return convertDatetimeString(new Date(System.currentTimeMillis()+secondsToShift*1000));
    }

    public static String convertDatetimeString(Date d) {
        SimpleDateFormat df = new SimpleDateFormat(EngineFactory.DATETIME_FORMAT);
        return df.format(d);
    }

    public static Map<String, Object> convertJSONObjectToMap(JSONObject obj) {
        if (obj==null || obj.size()==0) return null;

        Map<String, Object> result = new HashMap<>();
        Set<String> keys = obj.keySet();
        for (String key : keys) result.put(key, obj.get(key));

        return result;
    }

    public static JSONObject convertMapToJSONObject(Map<String, Object> obj) {
        if (obj==null || obj.size()==0) return null;

        JSONObject result = new JSONObject();
        Set<String> keys = obj.keySet();
        for (String key : keys) result.put(key, obj.get(key));

        return result;
    }

    /**
     * granularity: hour
     * ceiling: true - round to ceiling; false - round to floor
     */
    public static String roundDatetimeString(String time, String granularity, boolean ceiling) throws Exception {
        if (granularity.equals("hour")) {
            if (ceiling) {
                SimpleDateFormat df = new SimpleDateFormat(EngineFactory.DATETIME_FORMAT);
                Date d = df.parse(time);
                d = new Date( d.getTime() + 3600000l );
                return convertDatetimeString(d).substring(0,14)+"00:00";
            }
            else return time.substring(0,14)+"00:00";
        }
        else throw new UnsupportedOperationException();
    }

    public static String composeDatetimeString(int year, int month) {
        return "" + year + (month>9 ? "-"+month : "-0"+month);
    }

    public static String composeDatetimeString(int year, int month, int day) {
        return "" + year + (month>9 ? "-"+month : "-0"+month) + (day>9 ? "-"+day : "-0"+day);
    }

    public static String composeDatetimeString(int year, int month, int day, int hours, int minutes, int seconds) {
        StringBuilder result = new StringBuilder();
        result.append(year)
                .append('-').append(month>9 ? month : ("0"+month))
                .append('-').append(day>9 ? day : ("0"+day))
                .append(' ').append(hours>9 ? hours : ("0"+hours))
                .append(':').append(minutes>9 ? minutes : ("0"+minutes))
                .append(':').append(seconds>9 ? seconds : ("0"+seconds));
        return result.toString();
    }

    // Time difference in seconds.
    public static long getTimeDifference(String endTime, String startTime) throws Exception {
        SimpleDateFormat df = new SimpleDateFormat(EngineFactory.DATETIME_FORMAT);

        Date t1 = df.parse(endTime);
        Date t2 = df.parse(startTime);

        return (t1.getTime() - t2.getTime())/1000;
    }

    public static void composeWhereClauseFromStrings(String field, String values, StringBuilder whereClause, List<Object> params) {
        String[] arr = values.split(",");
        whereClause.append(field).append(" IN (");

        for (int i = 0; i < arr.length; i++) {
            if (i > 0) whereClause.append(",");
            whereClause.append("?");
            params.add(arr[i].trim());
        }

        whereClause.append(")");
    }

    public static void composeWhereClause(boolean[] hasCondition, String condition, Object value, StringBuilder sql, List<Object> params) {
        composeWhereClause(hasCondition,condition,value,"",sql,params);
    }

    public static void composeWhereClause(boolean[] hasCondition, String condition, Object value, String valueType, StringBuilder sql, List<Object> params) {
        if (value == null) return;

        if (hasCondition[0]) sql.append(" AND ");
        else hasCondition[0] = true;

        switch (valueType) {
            case "like":
                sql.append(condition).append(" LIKE ?");
                params.add("%"+value+"%");
                break;
            case "boolean":
                if ("true".equals(value))
                    sql.append(condition).append("=1");
                else
                    sql.append('(')
                            .append(condition).append(" IS NULL OR ")
                            .append(condition).append("=0")
                            .append(')');
                break;
            default:
                sql.append(condition).append("=?");
                params.add(value);
        }
    }

    public static <T> T[] removeItemsFromArray(T[] list, T[] items) {
        if (list==null || list.length==0 || items==null || items.length==0) return list;

        Set<T> setList = new HashSet<T>(Arrays.asList(list));
        Set<T> setItems = new HashSet<T>(Arrays.asList(items));
        setList.removeAll(setItems);

        T[] arr = (T[]) Array.newInstance(items[0].getClass(), 0);
        return setList.toArray(arr);
    }

    /**
     * Return the value of [{<field>: value}].
     * If there are more than one values (including null), return null
     */
    public static Object getCommonValueFromJSONArray(DataObject obj, String field) {
        if (obj == null) return null;

        JSONArray arr = obj.getJSONArray();
        if (arr==null || arr.size()==0) return null;

        Object result = null;
        boolean resultChanged = false;

        for (int i = 0; i < arr.size(); i++) {
            JSONObject item = arr.getJSONObject(i);
            Object v = item.get(field);

            if (!resultChanged && i>0 &&
                    (v!=null && result==null || result!=null && !result.equals(v))) resultChanged = true;
            result = v;
        }

        return resultChanged ? null : result;
    }

    public static Set<String> getValueSetFromJSONArray(DataObject obj, String field) {
        if (obj == null) return null;

        JSONArray arr = obj.getJSONArray();
        if (arr==null || arr.size()==0) return null;

        String strNull = "It's a null";
        Set<String> values = new HashSet<>();

        for (int i = 0; i < arr.size(); i++) {
            JSONObject item = arr.getJSONObject(i);
            String v = item.getString(field);
            values.add(v==null ? strNull : v);
        }

        return values;
    }

}
