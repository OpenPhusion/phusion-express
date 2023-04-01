package cloud.phusion.express.service;

import cloud.phusion.*;
import cloud.phusion.express.ExpressService;
import cloud.phusion.express.util.CommonCode;
import cloud.phusion.express.util.FullTextEncoder;
import cloud.phusion.express.util.ServiceLogger;
import cloud.phusion.storage.DBStorage;
import cloud.phusion.storage.Record;
import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONArray;
import com.alibaba.fastjson2.JSONObject;
import org.apache.catalina.startup.ExpandWar;

import java.text.SimpleDateFormat;
import java.util.*;

/**
 * For pre-calculated transaction statistics, the unfinished transactions will be taken as failed ones.
 * That is: failed = failed + count - finished; success = count - failed.
 * For real-time statistics, the unfinished will not be taked as failed.
 */
public class TransactionService implements ScheduledTask {
    private static final String _position = TransactionService.class.getName();

    private static final String STATS_TABLE = "transaction_stats";

    public static final String TRXLog_Enabled = "db.storage.trxLog.enabled";

    // In seconds. For calculating stats.
    public static final String TRXLog_Stats_Interval = "db.storage.trxLog.stats.intervalSeconds";

    private static boolean enabled = true;
    private static long statsInterval = 3600;

    public static void init(boolean trxLogEnabled, String strStatsInterval, boolean prepareTables, Context ctx) throws Exception {
        enabled = trxLogEnabled;
        statsInterval = Long.parseLong(strStatsInterval);

        if (! enabled) return;

        if (prepareTables) prepareDBTables(ctx);

        // Start the stats task
        if (statsInterval > 0) {
            ctx.getEngine().scheduleTask(
                    ExpressService.STORAGE_ID + "-trxstats",
                    new TransactionService(),
                    null, statsInterval, 0,
                    true, ctx
            );
        }
    }

    public static void prepareDBTables(Context ctx) throws Exception {
        DBStorage storage = ctx.getEngine().getDBStorageForApplication(ExpressService.STORAGE_ID);

        storage.prepareTable(
                STATS_TABLE,
                "{" +
                        "    \"fields\": {" +
                        "        \"integrationId\": \"String[50]\"," +
                        "        \"engineId\": \"String[10]\"," +
                        "        \"startTime\": \"String[20]\"," +
                        "        \"year\": \"Integer\"," +
                        "        \"month\": \"Integer\"," +
                        "        \"day\": \"Integer\"," +
                        "        \"hour\": \"Integer\"," +
                        "        \"count\": \"Integer\"," +
                        "        \"failed\": \"Integer\"," +
                        "        \"finished\": \"Integer\"," +
                        "        \"duration\": \"Double\"" +
                        "    }," +
                        "    \"indexes\": [" +
                        "        {\"field\": \"integrationId\"}," +
                        "        {\"field\": \"startTime\"}," +
                        "        {\"fields\": [\"year\",\"month\",\"day\",\"hour\"]}" +
                        "    ]" +
                        "}",
                ctx
        );
    }

    private static final String sqlStatsTask = "INSERT INTO " + ExpressService.STORAGE_ID + "_" + STATS_TABLE + " SELECT " +
            "integrationId, engineId, concat(substring(min(startTime),1,13),':00:00') as startTime, " +
            "year(startTime) as year, month(startTime) as month, day(startTime) as day, hour(startTIme) as hour, " +
            "count(1) as count, sum(failed) as failed, sum(finished) as finished, sum(duration) as duration " +
            "FROM " + ExpressService.STORAGE_ID + "_" + EngineFactory.TRXLog_Table_Transaction + " WHERE " +
            "startTime>=? AND startTime<? GROUP BY " +
            "integrationId, engineId, year(startTime), month(startTime), day(startTime), hour(startTIme)";

    public static void calculateStats(Context ctx) throws Exception {
        if (! enabled) throw new PhusionException("TRX_LOG_NONE","");

        DBStorage storage = ctx.getEngine().getDBStorageForApplication(ExpressService.STORAGE_ID);

        // Get the time range: from DB's last record to current time minus "statsInterval"

        String startTime = _getLastStatsTime(storage, ctx);
        String endTime = CommonCode.roundDatetimeString(CommonCode.convertDatetimeString(-statsInterval), "hour", false);

        if (startTime.compareTo(endTime) < 0) { // If startTime < endTime
            List<Object> params = new ArrayList<>();
            params.add(startTime);
            params.add(endTime);

            storage.freeUpdate(sqlStatsTask, params, ctx);
        }
    }

    public static DataObject getTransactionStats(List<String> whereFields, List<Object> whereParams, Context ctx) throws Exception {
        if (! enabled) throw new PhusionException("TRX_LOG_NONE","");
        return _getTransactionStats(whereFields, whereParams, true, null, ctx);
    }

    public static DataObject getTransactionGroupStats(List<String> whereFields, List<Object> whereParams, String groupBy, Context ctx) throws Exception {
        if (! enabled) throw new PhusionException("TRX_LOG_NONE","");
        return _getTransactionStats(whereFields, whereParams, false, groupBy, ctx);
    }

    private static DataObject _getTransactionStats(List<String> whereFields, List<Object> whereParams, boolean statsOnly, String groupBy, Context ctx) throws Exception {
        // Split query to pre-calculated stats table (3*statsInterval ago),
        // and realtime transaction table (after 3*statsInterval).

        // First, query the stats table.

        List<String> whereFieldsStats = new ArrayList<>();
        if (whereFields != null) whereFieldsStats.addAll(whereFields);
        else whereFields = new ArrayList<>();

        List<Object> paramsStats = new ArrayList<>();
        if (whereParams != null) paramsStats.addAll(whereParams);
        else whereParams = new ArrayList<>();

        _prepareQueryTime(whereFieldsStats, paramsStats, true);

        String sqlStats = _composeTrxQuerySQL(null, whereFieldsStats, paramsStats, groupBy, statsOnly, true);

        DataObject dataStats = null;
        if (sqlStats != null) {
            dataStats = _queryTransactionStats(sqlStats, paramsStats, groupBy, ctx);

            // For history (non-realtime) data, the unfinished transaction is taken as failed ones.
            _rearrangeStats(dataStats, groupBy, true);
        }

        // Then, query the realtime table.

        // If the first part (querying stats table) not been performed,
        // perform the full query over the realtime table.
        List<String> whereFieldsRealTime = whereFields;
        List<Object> paramsRealtime = whereParams;

        if (sqlStats != null) _prepareQueryTime(whereFieldsRealTime, paramsRealtime, false);

        String sqlRealtime = _composeTrxQuerySQL(null, whereFieldsRealTime, paramsRealtime, groupBy, statsOnly, false);

        DataObject dataRealtime = null;
        if (sqlRealtime != null) {
            dataRealtime = _queryTransactionStats(sqlRealtime, paramsRealtime, groupBy, ctx);
            _rearrangeStats(dataRealtime, groupBy, false);
        }

        return _mergeStats(dataStats, dataRealtime, groupBy);
    }

    private static void _prepareQueryTime(List<String> fields, List<Object> params, boolean useStatsTable) {
        String time = CommonCode.convertDatetimeString(-3 * statsInterval);
        String timeField = useStatsTable ? "endTime" : "startTime";
        int pos = -1;

        for (int i = 0; i < fields.size(); i++) {
            if (timeField.equals(fields.get(i))) {
                pos = i;
                break;
            }
        }
        if (pos < 0) {
            fields.add(timeField);
            params.add(time);
        }
        else
            params.set(pos, time);
    }

    private static DataObject _queryTransactionStats(String sql, List<Object> params, String groupBy, Context ctx) throws Exception {
        DBStorage storage = ctx.getEngine().getDBStorageForApplication(ExpressService.STORAGE_ID);
        Record[] records = storage.freeQuery(sql, params, 0, 10000, ctx);

        if (records==null || records.length==0) return null;
        else {
            JSONObject result = new JSONObject();

            if (groupBy != null) {
                String groupField = null;

                switch (groupBy) {
                    case "integration": groupField = "integrationId"; break;
                    case "engine": groupField = "engineId"; break;
                    case "client": groupField = "clientId"; break;
                    case "application": groupField = "applicationId"; break;
                    case "year": groupField = "year"; break;
                    case "hourOnly": groupField = "hour"; break;
                    case "month":
                    case "day":
                    case "hour":
                    case "minute": groupField = "time"; break;
                }

                for (Record record : records) {
                    String group = null;
                    if ("time".equals(groupField)) group = _getTimeGroupTitle(record, groupBy);
                    else group = record.getString(groupField);

                    JSONObject item = new JSONObject();
                    item.put("count", record.getInteger("count"));
                    item.put("failed", record.getInteger("failed"));
                    item.put("finished", record.getInteger("finished"));
                    item.put("duration", record.getDouble("duration"));

                    result.put(group, item);
                }
            }
            else {
                Record record = records[0];
                result.put("count", record.getInteger("count"));

                if (record.doesFieldExist("finished")) { // For "search", there's only "count" returned.
                    result.put("failed", record.getInteger("failed"));
                    result.put("finished", record.getInteger("finished"));
                    result.put("duration", record.getDouble("duration"));
                }
            }

            return new DataObject(result);
        }
    }

    private static String _getTimeGroupTitle(Record record, String groupBy) {
        String result = null;
        int iy = record.getInteger("year");
        int im = record.getInteger("month");
        Integer d = record.getInteger("day");
        int id = d==null ? 0 : d;
        Integer h = record.getInteger("hour");
        int ih = h==null ? 0 : h;
        Integer n = record.getInteger("minute");
        int in = n==null ? 0 : n;

        switch (groupBy) {
            case "month": result = CommonCode.composeDatetimeString(iy,im); break;
            case "day": result = CommonCode.composeDatetimeString(iy,im,id); break;
            case "hour":
            case "minute": result = CommonCode.composeDatetimeString(iy,im,id,ih,in,0); break;
        }

        return result;
    }

    private static void _rearrangeStats(DataObject stats, String groupBy, boolean treatUnfinishedAsFailed) {
        if (stats == null) return;
        JSONObject statsObj = stats.getJSONObject();
        Set<String> groups;

        if (groupBy == null) {
            groups = new HashSet<>();
            groups.add("");
        }
        else
            groups = statsObj.keySet();

        for (String group : groups) {
            JSONObject obj = (group.length()==0) ? statsObj : statsObj.getJSONObject(group);
            if (! obj.containsKey("finished")) continue;

            int count = obj.getIntValue("count", 0);
            int failedCount = obj.getIntValue("failed", 0);
            int finishedCount = obj.getIntValue("finished", 0);
            int unfinishedCount = count - finishedCount;
            int successCount = finishedCount - failedCount;

            if (treatUnfinishedAsFailed) {
                failedCount += unfinishedCount;
                unfinishedCount = 0;
            }

            obj.remove("failed");
            obj.remove("finished");

            obj.put("successCount", successCount);
            obj.put("failedCount", failedCount);
            obj.put("unfinishedCount", unfinishedCount);
        }

        stats.setData(statsObj);
    }

    private static DataObject _mergeStats(DataObject dataStats, DataObject dataRealtime, String groupBy) {
        if (dataStats==null && dataRealtime!=null) return dataRealtime;
        if (dataRealtime==null && dataStats!=null) return dataStats;
        if (dataRealtime==null && dataStats==null) return null;

        JSONObject result = dataStats.getJSONObject();
        JSONObject objRealtime = dataRealtime.getJSONObject();
        Set<String> groups;

        if (groupBy == null) {
            groups = new HashSet<>();
            groups.add("");
        }
        else
            groups = objRealtime.keySet();

        for (String group : groups) {
            JSONObject itemResult = null;
            JSONObject item = null;

            if (group.length() == 0) {
                itemResult = result;
                item = objRealtime;
            }
            else {
                itemResult = result.getJSONObject(group);
                item = objRealtime.getJSONObject(group);
            }

            if (itemResult == null)
                result.put(group, item);
            else {
                itemResult.put("count", itemResult.getIntValue("count",0) + item.getIntValue("count",0));
                itemResult.put("successCount", itemResult.getIntValue("successCount",0) + item.getIntValue("successCount",0));
                itemResult.put("failedCount", itemResult.getIntValue("failedCount",0) + item.getIntValue("failedCount",0));
                itemResult.put("unfinishedCount", itemResult.getIntValue("unfinishedCount",0) + item.getIntValue("unfinishedCount",0));
                itemResult.put("duration", itemResult.getDoubleValue("duration") + item.getDoubleValue("duration"));
            }
        }

        return new DataObject(result);
    }

    private static final String trxDefaultFields = "id,integrationId,failed,finished,startTime,duration,engineId";
    private static final Map<String,String> trxSpecialFields = new HashMap<>();

    static {
        trxSpecialFields.put("id", "String");
        trxSpecialFields.put("failed", "Boolean");
        trxSpecialFields.put("finished", "Boolean");
    }

    public static DataObject listTransactions(String fields, List<String> whereFields, List<Object> params, long from, long length, Context ctx) throws Exception {
        if (! enabled) throw new PhusionException("TRX_LOG_NONE","");

        Set<String> fieldSet = CommonCode.convertStringToSet(fields, "id", trxDefaultFields);
        String sql = _composeTrxQuerySQL(fieldSet, whereFields, params, null, false, false);

        return _queryTransactions(sql, params, from, length, fieldSet, ctx);
    }

    public static DataObject listTransactionsById(String ids, String fields, Context ctx) throws Exception {
        if (! enabled) throw new PhusionException("TRX_LOG_NONE","");
        if (ids==null || ids.length()==0) return null;

        StringBuilder sql = new StringBuilder();
        List<Object> params = new ArrayList<>();

        Set<String> fieldSet = CommonCode.convertStringToSet(fields, "id", trxDefaultFields);
        String selectClause = CommonCode.convertFieldSetToSelectClause(fieldSet);

        sql.append("SELECT ").append(selectClause)
                        .append(" FROM ").append(ExpressService.STORAGE_ID)
                        .append('_').append(EngineFactory.TRXLog_Table_Transaction)
                        .append(" WHERE true");
        CommonCode.composeWhereClauseFromStrings("id", ids, sql, params);

        return _queryTransactions(sql.toString(), params, 0, 1000, fieldSet, ctx);
    }

    private static DataObject _queryTransactions(String sql, List<Object> params, long from, long length, Set<String> fieldSet, Context ctx) throws Exception {
        DBStorage storage = ctx.getEngine().getDBStorageForApplication(ExpressService.STORAGE_ID);
        JSONArray result = new JSONArray();

        Record[] list = storage.freeQuery(sql, params, from, length, ctx);

        if (list!=null && list.length>0) {
            for (Record record : list) {
                JSONObject item = new JSONObject();
                CommonCode.copyRecordFields(record, item, fieldSet, trxSpecialFields);
                result.add(item);
            }
        }

        return new DataObject(result);
    }

    private static final String sqlTrxSteps = "SELECT S.step AS step,S.fromStep AS fromStep," +
            "S.duration AS duration,SI.config AS config,SI.msg AS msg,SI.properties AS properties " +
            "FROM phusion_transaction_step S JOIN phusion_transaction_step_info SI ON SI.stepId=S.id " +
            "WHERE S.transactionId=? ORDER BY S.id ASC";

    public static DataObject fetchTransaction(String transactionId, Context ctx) throws Exception {
        if (! enabled) throw new PhusionException("TRX_LOG_NONE","");

        JSONObject result = new JSONObject();
        DBStorage storage = ctx.getEngine().getDBStorageForApplication(ExpressService.STORAGE_ID);

        // Fetch transaction info

        Set<String> fieldSet = CommonCode.convertStringToSet(null, null, trxDefaultFields);

        Record record = storage.queryRecordById(
                EngineFactory.TRXLog_Table_Transaction,
                trxDefaultFields, "id", transactionId, ctx
        );

        if (record == null) return null;
        else CommonCode.copyRecordFields(record, result, fieldSet, trxSpecialFields);

        // Fetch steps

        List<Object> params = new ArrayList<>();
        params.add(transactionId);

        String config = null;
        Record[] records = storage.freeQuery(sqlTrxSteps, params, 0,1000, ctx);

        if (records!=null && records.length>0) {
            JSONArray arr = new JSONArray();

            for (Record r : records) {
                String v;
                JSONObject item = new JSONObject();

                item.put("duration", r.getDouble("duration"));

                v = r.getString("config");
                if (v!=null && v.length()>0) config = v;

                v = r.getString("step");
                if (v!=null && v.length()>0) item.put("to", v);

                v = r.getString("fromStep");
                if (v!=null && v.length()>0) item.put("from", v);

                v = r.getString("properties");
                if (v!=null && v.length()>0) item.put("properties", JSON.parseObject(v));

                v = r.getString("msg");
                if (v!=null && v.length()>0) {
                    v = FullTextEncoder.decode(v);
                    char c = v.charAt(0);
                    Object obj = c=='{' ? JSON.parseObject(v) : (c=='[' ? JSON.parseArray(v) : v);
                    item.put("msg", obj);
                }

                arr.add(item);
            }

            result.put("steps", arr);
        }

        if (config != null) result.put("config", JSON.parseObject(config));

        return new DataObject(result);
    }

    public static DataObject getTransactionStepStats(String integrationId, String startTime, String endTime, Context ctx) throws Exception {
        if (! enabled) throw new PhusionException("TRX_LOG_NONE","");

        StringBuilder sql = new StringBuilder();
        List<Object> params = new ArrayList<>();

        sql.append("SELECT step, fromStep, count(1) AS count, sum(S.duration) AS duration FROM ")
                .append(ExpressService.STORAGE_ID).append('_').append(EngineFactory.TRXLog_Table_Transaction)
                .append(" T JOIN ")
                .append(ExpressService.STORAGE_ID).append('_').append(EngineFactory.TRXLog_Table_TransactionStep)
                .append(" S ON S.transactionId=T.id WHERE T.integrationId=?");
        params.add(integrationId);

        if (startTime!=null && startTime.length()>0) {
            sql.append(" AND T.startTime>=?");
            params.add(startTime);
        }

        if (endTime!=null && endTime.length()>0) {
            sql.append(" AND T.startTime<?");
            params.add(endTime);
        }

        sql.append(" GROUP BY step, fromStep");

        DBStorage storage = ctx.getEngine().getDBStorageForApplication(ExpressService.STORAGE_ID);
        Record[] records = storage.freeQuery(sql.toString(), params, 0, 1000, ctx);

        StringBuilder result = new StringBuilder();
        result.append("[");

        if (records!=null && records.length>0) {
            for (int i = 0; i < records.length; i++) {
                Record r = records[i];
                if (i > 0) result.append(",");
                result.append("{");

                result.append("\"count\":").append(r.getLong("count"));
                result.append(",\"duration\":").append(r.getDouble("duration"));

                String step;
                step = r.getString("step");
                if (step!=null && step.length()>0)
                    result.append(",\"to\":\"").append(step).append("\"");
                step = r.getString("fromStep");
                if (step!=null && step.length()>0)
                    result.append(",\"from\":\"").append(step).append("\"");

                result.append("}");
            }
        }

        result.append("]");
        return new DataObject(result.toString());
    }

    /**
     * groupBy: integration, client, application, engine, year, month, day, hour, minute, hourOnly
     * whereFields: integrationId, clientId, applicationId, engineId, startTime, endTime, search
     * id,integrationId,failed,finished,time,duration
     * useStatsTable: true - query transaction_stats table; false - query transaction table
     * statsOnly: true - return stats; false - return records
     *
     * Aliases of tables:
     * T - transaction/transaction_stats
     * I - integration
     * IA - integration_application
     * S - transaction_step
     * SI - transaction_step_info
     */
    private static String _composeTrxQuerySQL(Set<String> selectFields, List<String> whereFields, List<Object> whereParams, String groupBy, boolean statsOnly, boolean useStatsTable) {
        StringBuilder result = new StringBuilder();
        Set<String> whereFieldsSet = new HashSet<>();

        if (groupBy!=null && groupBy.length()==0) groupBy = null;
        if (whereFields!=null && whereFields.size()==0) whereFields = null;
        if (whereFields != null) whereFieldsSet.addAll(whereFields);

        // Minutes not appear in the stats table.
        if (groupBy!=null && groupBy.equals("minute") && useStatsTable) return null;

        // If there's "search", can not use the stats table.
        if (whereFields!=null && whereFieldsSet.contains("search") && useStatsTable) return null;

        // If startTime >= endTime, do not perform the query.
        if (whereFieldsSet.contains("startTime") && whereFieldsSet.contains("endTime")) {
            String startTime = null;
            String endTime = null;

            for (int i = 0; i < whereFields.size(); i++) {
                if ("startTime".equals(whereFields.get(i))) startTime = (String) whereParams.get(i);
                else if ("endTime".equals(whereFields.get(i))) endTime = (String) whereParams.get(i);
            }

            if (startTime.compareTo(endTime) > 0) return null;
        }

        // Compose the SELECT clause

        result.append("SELECT ");
        if (whereFields!=null && whereFieldsSet.contains("search") && !statsOnly) result.append("DISTINCT ");

        if (statsOnly || groupBy!=null) {
            if (groupBy != null) {
                _composeGroupByFields(groupBy, useStatsTable, true, result);
                result.append(",");
            }

            String statsFields = ",sum(T.failed) AS failed,sum(T.finished) AS finished,sum(T.duration) AS duration";

            if (useStatsTable)
                result.append("sum(T.count) AS count").append(statsFields);
            else if (whereFields!=null && whereFieldsSet.contains("search"))
                result.append("count(DISTINCT T.id) AS count");
            else
                result.append("count(1) AS count").append(statsFields);
        }
        else {
            if (selectFields==null || selectFields.size()==0) return null;

            boolean isFirst = true;
            for (String field : selectFields) {
                if (isFirst) isFirst = false;
                else result.append(',');
                result.append("T.").append(field).append(" AS ").append(field);
            }
        }

        // Compose the FROM clause

        boolean needTableI = false;
        boolean needTableIA = false;
        boolean needTableS = false;
        boolean needTableSI = false;

        if (whereFields != null) {
            if (whereFieldsSet.contains("clientId")) needTableI = true;
            if (whereFieldsSet.contains("applicationId")) needTableIA = true;
            if (whereFieldsSet.contains("search")) {
                needTableS = true;
                needTableSI = true;
            }
        }

        if (groupBy != null) {
            if (groupBy.equals("client")) needTableI = true;
            if (groupBy.equals("application")) needTableIA = true;
        }

        result.append(" FROM ")
                .append(ExpressService.STORAGE_ID).append('_')
                .append(useStatsTable ? STATS_TABLE : EngineFactory.TRXLog_Table_Transaction)
                .append(" T");

        if (needTableI) {
            result.append(" JOIN ")
                    .append(ExpressService.STORAGE_ID).append('_')
                    .append(IntegrationService.INTEGRATION_TABLE)
                    .append(" I ON I.id=T.integrationId");
        }

        if (needTableIA) {
            result.append(" JOIN ")
                    .append(ExpressService.STORAGE_ID).append('_')
                    .append(IntegrationService.INTEGRATION_APP_TABLE)
                    .append(" IA ON T.integrationId=IA.integrationId");
        }

        if (needTableS) {
            result.append(" JOIN ")
                    .append(ExpressService.STORAGE_ID).append('_')
                    .append(EngineFactory.TRXLog_Table_TransactionStep)
                    .append(" S ON T.id=S.transactionId");
        }

        if (needTableSI) {
            result.append(" JOIN ")
                    .append(ExpressService.STORAGE_ID).append('_')
                    .append(EngineFactory.TRXLog_Table_TransactionStepInfo)
                    .append(" SI ON S.id=SI.stepId");
        }

        // Compose the WHERE clause

        if (whereFields !=  null) {
            result.append(" WHERE ");

            boolean isFirst = true;
            int timeParamPos = -1;

            for (int i = 0; i < whereFields.size(); i++) {
                String field = whereFields.get(i);

                if (isFirst) isFirst = false;
                else result.append(" AND ");

                switch (field) {
                    case "integrationId":
                        result.append("T.integrationId=?");
                        break;
                    case "clientId":
                        result.append("I.clientId=?");
                        break;
                    case "applicationId":
                        result.append("IA.applicationId=?");
                        break;
                    case "engineId":
                        result.append("T.engineId=?");
                        break;
                    case "startTime":
                        result.append("T.startTime>=?");
                        break;
                    case "endTime":
                        result.append("T.startTime<?");
                        break;
                    case "search":
                        result.append("MATCH SI.msg AGAINST (? IN BOOLEAN MODE)");
                        whereParams.set(i, FullTextEncoder.encodeAsQueryString((String)whereParams.get(i)));
                        break;
                }
            }
        }

        // Compose the GROUP BY clause

        if (groupBy != null) {
            result.append(" GROUP BY ");
            _composeGroupByFields(groupBy, useStatsTable, false, result);
        }

        // Compose the ORDER BY clause

        if (!statsOnly && groupBy==null) {
            result.append(" ORDER BY T.id DESC");
        }

        return result.toString();
    }

    private static void _composeGroupByFields(String groupBy, boolean useStatsTable, boolean addAlias, StringBuilder result) {
        switch (groupBy) {
            case "integration":
                result.append("T.integrationId");
                if (addAlias) result.append(" AS integrationId");
                break;
            case "engine":
                result.append("T.engineId");
                if (addAlias) result.append(" AS engineId");
                break;
            case "client":
                result.append("I.clientId");
                if (addAlias) result.append(" AS clientId");
                break;
            case "application":
                result.append("IA.applicationId");
                if (addAlias) result.append(" AS applicationId");
                break;
            case "year":
                if (useStatsTable) result.append("T.year");
                else result.append("year(T.startTime)");
                if (addAlias) result.append(" AS year");
                break;
            case "month":
                if (addAlias) {
                    if (useStatsTable) result.append("T.year AS year,T.month AS month");
                    else result.append("year(T.startTime) AS year,month(T.startTime) AS month");
                }
                else {
                    if (useStatsTable) result.append("T.year,T.month");
                    else result.append("year(T.startTime),month(T.startTime)");
                }
                break;
            case "day":
                if (addAlias) {
                    if (useStatsTable) result.append("T.year AS year,T.month AS month,T.day AS day");
                    else result.append("year(T.startTime) AS year,month(T.startTime) AS month,day(T.startTime) AS day");
                }
                else {
                    if (useStatsTable) result.append("T.year,T.month,T.day");
                    else result.append("year(T.startTime),month(T.startTime),day(T.startTime)");
                }
                break;
            case "hour":
                if (addAlias) {
                    if (useStatsTable) result.append("T.year AS year,T.month AS month,T.day AS day,T.hour AS hour");
                    else result.append("year(T.startTime) AS year,month(T.startTime) AS month,day(T.startTime) AS day,hour(T.startTime) AS hour");
                }
                else {
                    if (useStatsTable) result.append("T.year,T.month,T.day,T.hour");
                    else result.append("year(T.startTime),month(T.startTime),day(T.startTime),hour(T.startTime)");
                }
                break;
            case "minute":
                if (! useStatsTable) {
                    if (addAlias) result.append("year(T.startTime) AS year,month(T.startTime) AS month,day(T.startTime) AS day,hour(T.startTime) AS hour,minute(T.startTime) AS minute");
                    else result.append("year(T.startTime),month(T.startTime),day(T.startTime),hour(T.startTime),minute(T.startTime)");
                }
                else result.append("Minute_Not_In_Stats_Table");
                break;
            case "hourOnly":
                if (useStatsTable) result.append("T.hour");
                else result.append("hour(T.startTime)");
                if (addAlias) result.append(" AS hour");
                break;
        }
    }

    private static String _getLastStatsTime(DBStorage storage, Context ctx) throws Exception {
        Record[] records = storage.queryRecords(
                STATS_TABLE, "startTime", null,null,
                "startTime desc", 0, 1, ctx
        );

        if (records!=null && records.length>0) {
            String time = records[0].getString("startTime");
            if (time!=null && time.length()>0) {
                // Add one second to jump to the next hour (see the next line of code, ceiling=true).
                time = time.substring(0,time.length()-1)+"1";
                return CommonCode.roundDatetimeString(time, "hour", true);
            }
        }

        return CommonCode.roundDatetimeString(CommonCode.convertDatetimeString(-statsInterval*2), "hour", false);
    }

    @Override
    public void run(String taskId, Context ctx) {
        try {
            calculateStats(ctx);
        } catch (Exception ex) {
            ServiceLogger.error(_position, "Failed to calculate transaction stats", "traceId="+ctx.getId(), ex);
        }
    }

}
