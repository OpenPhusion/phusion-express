package cloud.phusion.express.component.storage;

import cloud.phusion.Context;
import cloud.phusion.EngineFactory;
import cloud.phusion.PhusionException;
import cloud.phusion.express.util.TimeMarker;
import cloud.phusion.storage.DBStorage;
import cloud.phusion.storage.Record;
import com.alibaba.druid.pool.DruidDataSourceFactory;
import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;

import javax.sql.DataSource;
import java.sql.*;
import java.util.*;

public class SQLDBStorageImpl implements DBStorage {
    private static final String _position = SQLDBStorageImpl.class.getName();

    private static String dbName = null;
    private static String dbUrl = null;
    private static String dbType = null; // dbType comes from JDBC URL, such as "mysql", "h2"

    private static DataSource ds = null;
    private static boolean isMemoryDB = false;
    private static Connection conn = null; // For in-memory database
    private static Set<String> tables = new HashSet<>(); // For in-memory database

    public static void init(Properties props) throws Exception {
        if (dbUrl!=null || props==null) return;

        String driver = props.getProperty(EngineFactory.JDBC_DriverClass);
        if (driver==null || driver.length()==0) return;

        String minPoolSize = props.getProperty(EngineFactory.JDBC_MinPoolSize,"2");
        String maxPoolSize = props.getProperty(EngineFactory.JDBC_MaxPoolSize,"200");
        String user = props.getProperty(EngineFactory.JDBC_User);
        String password = props.getProperty(EngineFactory.JDBC_Password);

        dbUrl = props.getProperty(EngineFactory.JDBC_Url);
        dbName = props.getProperty(EngineFactory.JDBC_DBName);
        dbType = (dbUrl.split(":")[1]).toLowerCase();
        isMemoryDB = dbType.equals("h2");

        if (isMemoryDB) {
            Driver d = (Driver) Class.forName(driver).newInstance();
            DriverManager.registerDriver(d);
            conn = DriverManager.getConnection(dbUrl);
        }
        else {
            Properties jdbcProps = new Properties();
            jdbcProps.setProperty("driverClassName", driver);
            jdbcProps.setProperty("url", dbUrl);
            jdbcProps.setProperty("username", user);
            jdbcProps.setProperty("password", password);
            jdbcProps.setProperty("initialSize", minPoolSize);
            jdbcProps.setProperty("maxActive", maxPoolSize);

            ds = DruidDataSourceFactory.createDataSource(jdbcProps);
            ds.getConnection().close(); // Try to connect to database. Or, the connection pool may be initialized in the web server threads.
        }
    }

    public static Connection getConnection() throws Exception {
        if (isMemoryDB) return conn;
        else return ds.getConnection();
    }

    public static void closeConnection(Connection conn) throws Exception {
        if (! isMemoryDB) {
            try {
                if (conn!=null && !conn.isClosed()) conn.close();
            }
            catch (Exception e) {}
        }
    }

    private String namespace;
    private Context baseCtx = null;

    /**
     * @param namespace A{applicationId}、I{integrationId}、C{clientId}
     */
    public SQLDBStorageImpl(String namespace, Context ctx) {
        super();

        this.namespace = namespace + "_";
        this.baseCtx = ctx==null ? EngineFactory.createContext() : ctx;
    }

    public SQLDBStorageImpl(String namespace) {
        this(namespace, null);
    }

    @Override
    public void prepareTable(String tableName, String schema) throws Exception {
        prepareTable(tableName, schema, baseCtx);
    }

    @Override
    public void prepareTable(String tableName, String schema, Context ctx) throws Exception {
        boolean exist = doesTableExist(tableName, ctx);

        SQLDBSchemaParser sqlParser = new SQLDBSchemaParser(dbType, isMemoryDB);
        String table = namespace + tableName;
        String sql;

        ctx.setContextInfo("tableName", table);

        try {
            ctx.logInfo(_position, "Preparing table", "schema="+schema);

            if (exist) {
                String dbSchema = sqlParser.getTableSchemaFromDB(table);
                ctx.logInfo(_position, "Retrieved table schema", "dbSchema="+dbSchema);

                sql = sqlParser.generateAlterTableStatement(table, dbSchema, schema);

                if (sql!=null && sql.length()>0) ctx.logInfo(_position, "Composed table alter sql", "sql=["+sql+"]");
                else ctx.logInfo(_position, "No change is needed");
            } else {
                sql = sqlParser.generateCreateTableStatement(table, schema);
                ctx.logInfo(_position, "Composed table create sql", "sql=["+sql+"]");
            }

            if (sql != null && sql.length() > 0) {
                Connection conn = getConnection();
                try {
                    try (Statement statement = conn.createStatement()) {
                        statement.execute(sql);

                        if (isMemoryDB) tables.add(table);
                        ctx.logInfo(_position, "SQL executed");
                    }
                } finally {
                    closeConnection(conn);
                }
            }

            ctx.logInfo(_position, "Table is ready");
        } catch (Exception ex) {
            throw new PhusionException("DB_OP", "Failed to prepare table", ctx, ex);
        }

        ctx.removeContextInfo("tableName");
    }

    @Override
    public void removeTable(String tableName) throws Exception {
        removeTable(tableName, baseCtx);
    }

    @Override
    public void removeTable(String tableName, Context ctx) throws Exception {
        ctx.setContextInfo("tableName", namespace + tableName);

        Connection conn = getConnection();
        try {
            try (Statement statement=conn.createStatement()) {
                statement.execute("DROP TABLE " + namespace + tableName);

                if (isMemoryDB) tables.remove(namespace + tableName);
                ctx.logInfo(_position, "Table is removed");
            }
        } catch (Exception ex) {
            throw new PhusionException("DB_OP", "Failed to drop table", String.format("namespace=%s, tableName=%s",
                    namespace, tableName), ctx, ex);
        } finally {
            closeConnection(conn);
        }

        ctx.removeContextInfo("tableName");
    }

    @Override
    public boolean doesTableExist(String tableName) throws Exception {
        return doesTableExist(tableName, baseCtx);
    }

    @Override
    public boolean doesTableExist(String tableName, Context ctx) throws Exception {
        if (isMemoryDB) {
            return tables.contains(namespace + tableName);
        }

        String tName = namespace + tableName;
        String[] types = new String[]{"TABLE"};

        Connection conn = getConnection();
        try {
            try (ResultSet tables=conn.getMetaData().getTables(dbName,null, tName, types)) {
                if (tables.next()) return true;
                else return false;
            }
        } catch (Exception ex) {
            throw new PhusionException("DB_OP", "Failed to check table", String.format("namespace=%s, tableName=%s",
                    namespace, tableName), ctx, ex);
        } finally {
            closeConnection(conn);
        }
    }

    @Override
    public int insertRecord(String tableName, Record record) throws Exception {
        return insertRecord(tableName, record, baseCtx);
    }

    @Override
    public int insertRecord(String tableName, Record record, Context ctx) throws Exception {
        ctx.setContextInfo("tableName", namespace + tableName);
        ArrayList<Object> arr = _recordToInsertStatement(record); // The first element is the SQL clause, and the others are argument values

        StringBuilder sql = new StringBuilder();
        sql.append("INSERT INTO ").append(namespace).append(tableName).append(" ");
        sql.append(arr.get(0));

        int count;

        ctx.logInfo(_position, "Inserting record", String.format("record=%s, sql=[%s], params=%s",
                record.toJSONString(), sql, arr.subList(1, arr.size())));
        TimeMarker marker = new TimeMarker();

        Connection conn = getConnection();
        try {
            try (PreparedStatement statement = conn.prepareStatement(sql.toString())) {
                for (int i = 1; i < arr.size(); i++) {
                    statement.setObject(i, arr.get(i));
                }
                count = statement.executeUpdate();

                double ms = marker.mark();
                ctx.logInfo(_position, "Record inserted", String.format("count=%d, time=%.1fms", count, ms));
            }
        } catch (Exception ex) {
            throw new PhusionException("DB_OP", "Failed to insert record", ctx, ex);
        } finally {
            closeConnection(conn);
        }

        ctx.removeContextInfo("tableName");
        return count;
    }

    @Override
    public int insertRecords(String tableName, String fields, List<Object> params, Context ctx) throws Exception {
        StringBuilder sql = new StringBuilder();
        sql.append("INSERT INTO ").append(namespace).append(tableName)
                .append(" (").append(fields).append(") VALUES ");

        int nFields = fields.split(",").length;
        int nParams = params.size();
        int nRecords = Math.floorDiv(nParams, nFields);
        StringBuilder record = new StringBuilder();
        record.append("(");
        for (int i = 0; i < nFields; i++) {
            if (i > 0) record.append(",");
            record.append("?");
        }
        record.append(")");
        String recordStr = record.toString();

        for (int i = 0; i < nRecords; i++) {
            if (i > 0) sql.append(",");
            sql.append(recordStr);
        }

        int count;

        ctx.logInfo(_position, "Inserting records", String.format("sql=[%s], params=%s", sql, Arrays.toString(params.toArray())));
        TimeMarker marker = new TimeMarker();

        Connection conn = getConnection();
        try {
            try (PreparedStatement statement = conn.prepareStatement(sql.toString())) {
                for (int i = 0; i < nParams; i++) {
                    statement.setObject(i+1, params.get(i));
                }
                count = statement.executeUpdate();

                double ms = marker.mark();
                ctx.logInfo(_position, "Records inserted", String.format("count=%d, time=%.1fms", count, ms));
            }
        } catch (Exception ex) {
            throw new PhusionException("DB_OP", "Failed to insert record", ctx, ex);
        } finally {
            closeConnection(conn);
        }

        return count;
    }

    private ArrayList<Object> _recordToInsertStatement(Record record) {
        ArrayList<Object> arr = new ArrayList<>();
        arr.add("");

        StringBuilder sql1 = new StringBuilder();
        StringBuilder sql2 = new StringBuilder();

        sql1.append("(");
        sql2.append(" VALUES (");
        boolean isStart = true;
        Set<String> fields = record.getFields();

        for (String field : fields) {
            if (! isStart) {
                sql1.append(", ");
                sql2.append(", ");
            }
            else isStart = false;

            sql1.append("`").append(field).append("`");
            sql2.append("?");

            arr.add(record.getValue(field));
        }

        sql1.append(")");
        sql2.append(")");
        arr.set(0, sql1.toString() + sql2.toString());
        return arr;
    }

    @Override
    public int upsertRecord(String tableName, String whereClause, List<Object> params, Record record) throws Exception {
        return upsertRecord(tableName,whereClause,params,record,false,baseCtx);
    }

    @Override
    public int upsertRecord(String tableName, String whereClause, List<Object> params, Record record, Context ctx) throws Exception {
        return upsertRecord(tableName,whereClause,params,record,false,ctx);
    }

    @Override
    public int upsertRecord(String tableName, String whereClause, List<Object> params, Record record, boolean noLog, Context ctx) throws Exception {
        // Not transactional, to be optimized

        List<Object> paramsUpdate = new ArrayList<>();
        paramsUpdate.addAll(params);

        long result = _queryCount(tableName, null, whereClause, params, true, ctx);
        if (result == 0) return insertRecord(tableName, record, ctx);
        else return _updateRecords(tableName, record, whereClause, paramsUpdate, noLog, ctx);
    }

    @Override
    public int upsertRecordById(String tableName, String idField, Record record) throws Exception {
        return upsertRecordById(tableName, idField, record, baseCtx);
    }

    @Override
    public int upsertRecordById(String tableName, String idField, Record record, Context ctx) throws Exception {
        Object value = record.getValue(idField.replaceAll("`",""));

        // Not transactional, to be optimized

        Record result = _queryRecordById(tableName, idField, idField, value, true, ctx);
        if (result == null) return insertRecord(tableName, record, ctx);
        else return updateRecordById(tableName, record, idField, value, ctx);
    }

    @Override
    public Record[] queryRecords(String tableName, String selectClause, String whereClause, String groupClause, String havingClause, List<Object> params, String orderClause, long from, long length) throws Exception {
        return _queryRecords(tableName, selectClause, whereClause, groupClause, havingClause, params, orderClause, from, length, false, baseCtx);
    }

    @Override
    public Record[] queryRecords(String tableName, String selectClause, String whereClause, String groupClause, String havingClause, List<Object> params, String orderClause, long from, long length, Context ctx) throws Exception {
        return _queryRecords(tableName,selectClause,whereClause,groupClause,havingClause,params,orderClause,from,length,false,ctx);
    }

    private Record[] _queryRecords(String tableName, String selectClause, String whereClause, String groupClause, String havingClause, List<Object> params, String orderClause, long from, long length, boolean noLog, Context ctx) throws Exception {
        ctx.setContextInfo("tableName", namespace + tableName);

        StringBuilder sql = new StringBuilder();
        sql.append("SELECT ")
                .append((selectClause==null || selectClause.length()==0) ? "*" : selectClause)
                .append(" FROM ").append(namespace).append(tableName);
        if (whereClause!=null && whereClause.length()>0) sql.append(" WHERE ").append(whereClause);
        if (groupClause!=null && groupClause.length()>0) sql.append(" GROUP BY ").append(groupClause);
        if (havingClause!=null && havingClause.length()>0) sql.append(" HAVING ").append(havingClause);
        if (orderClause!=null && orderClause.length()>0) sql.append(" ORDER BY ").append(orderClause);

        if (length > 0) {
            sql.append(" LIMIT ?");
            if (params == null) params = new ArrayList<>();
            params.add(length); // Changed the input object. To be optimized.
        }
        if (from > 0) {
            sql.append(" OFFSET ?");
            if (params == null) params = new ArrayList<>();
            params.add(from); // Changed the input object. To be optimized.
        }

        if (! noLog) ctx.logInfo(_position, "Executing query", String.format("sql=[%s], params=%s",
                sql, params!=null ? Arrays.toString(params.toArray()) : "null"));
        TimeMarker marker = new TimeMarker();

        ArrayList<Record> result = new ArrayList<>();

        Connection conn = getConnection();
        try {
            try (PreparedStatement statement = conn.prepareStatement(sql.toString())) {
                _fillUpParams(statement, 0, params);

                try (ResultSet records = statement.executeQuery()) {
                    while (records.next()) {
                        result.add( _resultSetToRecord(records) );
                    }

                    double ms = marker.mark();
                    if (! noLog) ctx.logInfo(_position, "Query executed", String.format("count=%d, time=%.1fms", result.size(), ms));
                }
            }
        } catch (Exception ex) {
            throw new PhusionException("DB_OP", "Failed to query", ctx, ex);
        } finally {
            closeConnection(conn);
        }

        ctx.removeContextInfo("tableName");
        return result.size()==0 ? null : result.toArray(new Record[]{});
    }

    @Override
    public Record[] freeQuery(String sql, List<Object> params, long from, long length, Context ctx) throws Exception {
        if (length > 0) {
            sql += " LIMIT ?";
            if (params == null) params = new ArrayList<>();
            params.add(length); // Changed the input object. To be optimized.
        }
        if (from > 0) {
            sql += " OFFSET ?";
            if (params == null) params = new ArrayList<>();
            params.add(from); // Changed the input object. To be optimized.
        }

        ctx.logInfo(_position, "Executing free query", String.format("sql=[%s], params=%s",
                sql, params!=null ? Arrays.toString(params.toArray()) : "null"));
        TimeMarker marker = new TimeMarker();

        ArrayList<Record> result = new ArrayList<>();

        Connection conn = getConnection();
        try {
            try (PreparedStatement statement = conn.prepareStatement(sql)) {
                _fillUpParams(statement, 0, params);

                try (ResultSet records = statement.executeQuery()) {
                    while (records.next()) {
                        result.add( _resultSetToRecord(records) );
                    }

                    double ms = marker.mark();
                    ctx.logInfo(_position, "Free query executed", String.format("count=%d, time=%.1fms", result.size(), ms));
                }
            }
        } catch (Exception ex) {
            throw new PhusionException("DB_OP", "Failed to free-query", ctx, ex);
        } finally {
            closeConnection(conn);
        }

        return result.size()==0 ? null : result.toArray(new Record[]{});
    }

    @Override
    public int freeUpdate(String sql, List<Object> params, Context ctx) throws Exception {
        ctx.logInfo(_position, "Executing free update", String.format("sql=[%s], params=%s",
                sql, params!=null ? Arrays.toString(params.toArray()) : "null"));
        TimeMarker marker = new TimeMarker();

        Connection conn = getConnection();
        try {
            try (PreparedStatement statement = conn.prepareStatement(sql)) {
                _fillUpParams(statement, 0, params);

                int result = statement.executeUpdate();

                double ms = marker.mark();
                ctx.logInfo(_position, "Free query executed", String.format("count=%d, time=%.1fms", result, ms));
                return result;
            }
        } catch (Exception ex) {
            throw new PhusionException("DB_OP", "Failed to free-query", ctx, ex);
        } finally {
            closeConnection(conn);
        }
    }

    @Override
    public Record[] queryRecords(String tableName, String selectClause, String whereClause, List<Object> params, String orderClause, long from, long length) throws Exception {
        return _queryRecords(tableName, selectClause, whereClause, null, null, params, orderClause, from, length, false, baseCtx);
    }

    @Override
    public Record[] queryRecords(String tableName, String selectClause, String whereClause, List<Object> params, String orderClause, long from, long length, Context ctx) throws Exception {
        return _queryRecords(tableName, selectClause, whereClause, null, null, params, orderClause, from, length, false, ctx);
    }

    private Record _resultSetToRecord(ResultSet resultSet) throws Exception {
        ResultSetMetaData metaData = resultSet.getMetaData();
        int columns = metaData.getColumnCount();

        Record record = new Record();

        for (int i = 1; i <= columns; i++) {
            record.setValue(metaData.getColumnName(i), resultSet.getObject(i));
        }

        return record;
    }

    @Override
    public Record[] queryRecords(String tableName, String selectClause, String whereClause, List<Object> params) throws Exception {
        return _queryRecords(tableName, selectClause, whereClause, null, null, params, null, 0, 100, false, baseCtx);
    }

    @Override
    public Record[] queryRecords(String tableName, String selectClause, String whereClause, List<Object> params, Context ctx) throws Exception {
        return _queryRecords(tableName, selectClause, whereClause, null, null, params, null, 0, 100, false, ctx);
    }

    @Override
    public Record queryRecordById(String tableName, String selectClause, String idField, Object value) throws Exception {
        return _queryRecordById(tableName, selectClause, idField, value, false, baseCtx);
    }

    @Override
    public Record queryRecordById(String tableName, String selectClause, String idField, Object value, Context ctx) throws Exception {
        return _queryRecordById(tableName, selectClause, idField, value, false, ctx);
    }

    private Record _queryRecordById(String tableName, String selectClause, String idField, Object value, boolean noLog, Context ctx) throws Exception {
        ArrayList<Object> params = new ArrayList<>();
        params.add(value);

        Record[] records = _queryRecords(tableName, selectClause, idField+"=?", null, null, params, null, 0, 1, noLog, ctx);
        if (records!=null && records.length>0) return records[0];
        else return null;
    }

    @Override
    public long queryCount(String tableName, String selectClause, String whereClause, List<Object> params) throws Exception {
        return _queryCount(tableName,selectClause,whereClause,params,false,baseCtx);
    }

    @Override
    public long queryCount(String tableName, String selectClause, String whereClause, List<Object> params, Context ctx) throws Exception {
        return _queryCount(tableName,selectClause,whereClause,params,false,ctx);
    }

    private long _queryCount(String tableName, String selectClause, String whereClause, List<Object> params, boolean noLog, Context ctx) throws Exception {
        String sqlSelect = "count(1) AS c";
        if (selectClause!=null && selectClause.trim().toLowerCase().indexOf("distinct ") == 0)
            sqlSelect = "count(" + selectClause + ") AS c";

        Record[] records = _queryRecords(tableName, sqlSelect, whereClause, null, null, params, null, 0, 1, noLog, ctx);
        if (records.length == 0) return 0;
        else return records[0].getLong("c");
    }

    @Override
    public int updateRecords(String tableName, Record record, String whereClause, List<Object> params) throws Exception {
        return _updateRecords(tableName,record,whereClause,params,false,baseCtx);
    }

    @Override
    public int updateRecords(String tableName, Record record, String whereClause, List<Object> params, Context ctx) throws Exception {
        return _updateRecords(tableName,record,whereClause,params,false,ctx);
    }

    private int _updateRecords(String tableName, Record record, String whereClause, List<Object> params, boolean noLog, Context ctx) throws Exception {
        ctx.setContextInfo("tableName", namespace + tableName);
        ArrayList<Object> arr = _recordToUpdateStatement(record); // The first element is the SQL clause, and the others are argument values

        StringBuilder sql = new StringBuilder();
        sql.append("UPDATE ").append(namespace).append(tableName).append(" SET ");
        sql.append(arr.get(0));
        if (whereClause!=null && whereClause.length()>0) sql.append(" WHERE ").append(whereClause);

        int count;

        ArrayList<Object> allParams = new ArrayList<>();
        allParams.addAll(arr.subList(1, arr.size()));
        if (params != null) allParams.addAll(params);
        if (! noLog) ctx.logInfo(_position, "Executing update", String.format("record=%s, sql=[%s], params=%s",
                record.toJSONString(), sql, allParams));
        TimeMarker marker = new TimeMarker();

        Connection conn = getConnection();
        try {
            try (PreparedStatement statement = conn.prepareStatement(sql.toString())) {
                for (int i = 1; i < arr.size(); i++) {
                    statement.setObject(i, arr.get(i));
                }

                _fillUpParams(statement, arr.size()-1, params);
                count = statement.executeUpdate();

                double ms = marker.mark();
                if (! noLog) ctx.logInfo(_position, "Update executed", String.format("count=%d, time=%.1fms", count, ms));
            }
        } catch (Exception ex) {
            throw new PhusionException("DB_OP", "Failed to update", ctx, ex);
        } finally {
            closeConnection(conn);
        }

        ctx.removeContextInfo("tableName");
        return count;
    }

    private ArrayList<Object> _recordToUpdateStatement(Record record) {
        ArrayList<Object> arr = new ArrayList<>();
        arr.add("");

        StringBuilder sql = new StringBuilder();
        boolean isStart = true;
        Set<String> fields = record.getFields();

        for (String field : fields) {
            if (! isStart) sql.append(", ");
            else isStart = false;

            sql.append("`").append(field).append("`").append("=?");
            arr.add(record.getValue(field));
        }

        arr.set(0, sql.toString());
        return arr;
    }

    @Override
    public int updateRecordById(String tableName, Record record, String idField, Object value) throws Exception {
        ArrayList<Object> params = new ArrayList<>();
        params.add(value);

        return _updateRecords(tableName, record, idField+"=?", params, false, baseCtx);
    }

    @Override
    public int updateRecordById(String tableName, Record record, String idField, Object value, Context ctx) throws Exception {
        ArrayList<Object> params = new ArrayList<>();
        params.add(value);

        return _updateRecords(tableName, record, idField+"=?", params, false, ctx);
    }

    @Override
    public int replaceRecordById(String tableName, Record record, String idField, Object value) throws Exception {
        return replaceRecordById(tableName, record, idField, value, baseCtx);
    }

    @Override
    public int replaceRecordById(String tableName, Record record, String idField, Object value, Context ctx) throws Exception {
        // Not transactional, and there's re-indexing impact, to be optimized

        deleteRecordById(tableName, idField, value, ctx);
        return insertRecord(tableName, record, ctx);
    }

    @Override
    public int deleteRecords(String tableName, String whereClause, List<Object> params) throws Exception {
        return deleteRecords(tableName, whereClause, params, baseCtx);
    }

    @Override
    public int deleteRecords(String tableName, String whereClause, List<Object> params, Context ctx) throws Exception {
        ctx.setContextInfo("tableName", namespace + tableName);

        StringBuilder sql = new StringBuilder();
        sql.append("DELETE FROM ").append(namespace).append(tableName);
        if (whereClause!=null && whereClause.length()>0) sql.append(" WHERE ").append(whereClause);

        int count;

        ctx.logInfo(_position, "Executing deletion", String.format("sql=[%s], params=%s",
                sql, params!=null ? Arrays.toString(params.toArray()) : "null"));
        TimeMarker marker = new TimeMarker();

        Connection conn = getConnection();
        try {
            try (PreparedStatement statement = conn.prepareStatement(sql.toString())) {
                _fillUpParams(statement, 0, params);
                count = statement.executeUpdate();

                double ms = marker.mark();
                ctx.logInfo(_position, "Deletion executed", String.format("count=%d, time=%.1fms", count, ms));
            }
        } catch (Exception ex) {
            throw new PhusionException("DB_OP", "Failed to delete", ctx, ex);
        } finally {
            closeConnection(conn);
        }

        ctx.removeContextInfo("tableName");
        return count;
    }

    @Override
    public int deleteRecordById(String tableName, String idField, Object value) throws Exception {
        ArrayList<Object> params = new ArrayList<>();
        params.add(value);

        return deleteRecords(tableName, idField+"=?", params, baseCtx);
    }

    @Override
    public int deleteRecordById(String tableName, String idField, Object value, Context ctx) throws Exception {
        ArrayList<Object> params = new ArrayList<>();
        params.add(value);

        return deleteRecords(tableName, idField+"=?", params, ctx);
    }

    private void _fillUpParams(PreparedStatement statement, int filledParams, List<Object> params) throws Exception {
        if (params==null || params.size()==0) return;

        for (int i = 0; i < params.size(); i++) {
            statement.setObject(i+(filledParams+1), params.get(i));
        }
    }

}
