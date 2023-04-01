package cloud.phusion.express.component.storage;

import cloud.phusion.PhusionException;
import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONArray;
import com.alibaba.fastjson2.JSONObject;

import javax.sql.DataSource;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

public class SQLDBSchemaParser {

    // If the SQL statement is related to the physical database, use dbType to identify the database system.
    private String dbType;
    private boolean isMemoryDB;

    private int stringDefaultLength;
    private int longStringMinLength;

    public SQLDBSchemaParser(String dbType, boolean isMemoryDB) {
        super();

        this.stringDefaultLength = 255;
        this.longStringMinLength = 3000;
        this.dbType = dbType;
        this.isMemoryDB = isMemoryDB;
    }

    public String getTableSchemaFromDB(String tablename) throws Exception {
        if (isMemoryDB) return null;

        StringBuilder result = new StringBuilder();
        result.append("{")
                .append("\"fields\":{");

        // Fetch information about the columns (fields)

        Connection conn = SQLDBStorageImpl.getConnection();
        try {
            try (Statement statement = conn.createStatement()) {
                try (ResultSet records = statement.executeQuery("SELECT * FROM "+tablename+" LIMIT 0")) {
                    ResultSetMetaData metaData = records.getMetaData();
                    for (int i = 1; i <= metaData.getColumnCount(); i++) {
                        int type = metaData.getColumnType(i);
                        String typeName;

                        switch (type) {
                            case Types.BIGINT: typeName = "Long"; break;
                            case Types.INTEGER: typeName = "Integer"; break;
                            case Types.TINYINT:
                            case Types.BOOLEAN:
                            case Types.BIT:
                                typeName = "Boolean"; break;
                            case Types.FLOAT:
                            case Types.REAL:
                                typeName = "Float"; break;
                            case Types.DOUBLE:
                            case Types.DECIMAL:
                            case Types.NUMERIC:
                                typeName = "Double"; break;
                            case Types.VARCHAR:
                            case Types.LONGVARCHAR: // i.e. TEXT
                                typeName = "String"; break;
                            default: typeName = "Other";
                        }

                        if (i > 1) result.append(",");
                        result.append("\"").append(metaData.getColumnName(i)).append("\":\"");
                        result.append(typeName);
                        if (typeName.equals("String")) result.append("[").append(metaData.getColumnDisplaySize(i)).append("]");
                        result.append("\"");
                    }
                }
            }
        } finally {
            SQLDBStorageImpl.closeConnection(conn);
        }

        result.append("},");

        // Fetch information about the indexes

        // Maps: index_name -> fields, index_name -> isPrimary, index_name -> isUnique, index_name -> isFulltext
        HashMap<String, ArrayList<String>> indexFields = new HashMap<>();
        HashMap<String, Boolean> indexPrimary = new HashMap<>();
        HashMap<String, Boolean> indexUnique = new HashMap<>();
        HashMap<String, Boolean> indexFulltext = new HashMap<>();

        conn = SQLDBStorageImpl.getConnection();
        try {
            DatabaseMetaData dbMetaData = conn.getMetaData();
            try (ResultSet indexes = dbMetaData.getIndexInfo(null,null,tablename,false,false)) {
                while (indexes.next()) {
                    String indexName = indexes.getString("INDEX_NAME");
                    indexPrimary.put(indexName, indexName.equals("PRIMARY"));
                    indexUnique.put(indexName, ! indexes.getBoolean("NON_UNIQUE"));

                    if (dbType.equals("mysql")) {
                        // If ASC_OR_DESC is null, then the index is fulltext. This trick may work for mysql only !!!
                        indexFulltext.put(indexName, indexes.getString("ASC_OR_DESC") == null);
                    }
                    else indexFulltext.put(indexName, Boolean.FALSE);

                    ArrayList<String> fields = indexFields.get(indexName);
                    if (fields == null) {
                        fields = new ArrayList<>();
                        indexFields.put(indexName, fields);
                    }
                    fields.add(indexes.getString("COLUMN_NAME"));
                }
            }
        } finally {
            SQLDBStorageImpl.closeConnection(conn);
        }

        result.append("\"indexes\":[");
        Set<String> indexes = indexFields.keySet();
        boolean isFirst = true;
        for (String index : indexes) {
            if (! isFirst) result.append(",");
            else isFirst = false;

            result.append("{");

            ArrayList<String> fields = indexFields.get(index);
            if (fields.size() == 1)
                result.append("\"field\":\"").append(fields.get(0)).append("\",");
            else {
                result.append("\"fields\":[");
                for (int i = 0; i < fields.size(); i++) {
                    if (i > 0) result.append(",");
                    result.append("\"").append(fields.get(i)).append("\"");
                }
                result.append("],");
            }

            result.append("\"primary\":").append(indexPrimary.get(index)).append(",");
            result.append("\"unique\":").append(indexUnique.get(index)).append(",");
            result.append("\"fulltext\":").append(indexFulltext.get(index));
            result.append("}");
        }
        result.append("]");

        result.append("}");

//        System.out.println(result);
        return result.toString();
    }

    public String generateCreateTableStatement(String tablename, String schema) throws Exception {
        if (isMemoryDB) {
            StringBuilder sql = new StringBuilder();
            JSONObject objSchema = JSON.parseObject(schema);
            JSONObject objFields = objSchema.getJSONObject("fields");
            boolean isFirst = true;

            sql.append("CREATE TABLE IF NOT EXISTS ").append(tablename).append(" (");

            if (objFields != null) {
                Set<String> fields = objFields.keySet();

                for (String field : fields) {
                    if (isFirst) isFirst = false;
                    else sql.append(", ");
                    sql.append("`").append(field).append("`");

                    String dataType = objFields.getString(field).toLowerCase();
                    sql.append(" ").append(_toDBDataType(dataType));
                    sql.append(" DEFAULT NULL");
                }
            }

            sql.append(")");
            return sql.toString();
        }

        StringBuilder sql = new StringBuilder();
        JSONObject objSchema = JSON.parseObject(schema);
        Set<String> primaryFields = new HashSet<>();
        boolean isFirst = true;

        sql.append("CREATE TABLE IF NOT EXISTS ").append(tablename).append(" (");

        // Get the primary columns

        JSONArray indexes = objSchema.getJSONArray("indexes");
        if (indexes!=null && indexes.size()>0) {
            for (int i = 0; i < indexes.size(); i++) {
                JSONObject index = indexes.getJSONObject(i);
                if (index.getBooleanValue("primary", false)) {
                    if (index.containsKey("field"))
                        primaryFields.add(index.getString("field").toLowerCase());
                    else {
                        JSONArray arr = index.getJSONArray("fields");
                        if (arr!=null && arr.size()>0) {
                            for (int j = 0; j <arr.size(); j++) primaryFields.add(arr.getString(j).toLowerCase());
                        }
                    }
                }
            }
        }

        // Definition statements for the columns

        JSONObject objFields = objSchema.getJSONObject("fields");

        if (objFields != null) {
            Set<String> fields = objFields.keySet();

            for (String field : fields) {
                if (isFirst) isFirst = false;
                else sql.append(", ");
                sql.append("`").append(field).append("`");

                String dataType = objFields.getString(field).toLowerCase();
                sql.append(" ").append(_toDBDataType(dataType));

                if (primaryFields.contains(field.toLowerCase())) sql.append(" NOT NULL");
                else sql.append(" DEFAULT NULL");
            }
        }

        // Definition statements for the indexes

        if (indexes!=null && indexes.size()>0) {
            for (int i = 0; i < indexes.size(); i++) {
                if (isFirst) isFirst = false;
                else sql.append(", ");

                JSONObject index = indexes.getJSONObject(i);
                boolean primary = index.getBooleanValue("primary", false);
                boolean unique = index.getBooleanValue("unique", false);
                boolean fulltext = index.getBooleanValue("fulltext", false);
                JSONArray arr = index.containsKey("fields") ?
                        index.getJSONArray("fields") :
                        new JSONArray(index.getString("field"));

                if (fulltext) {
                    sql.append("FULLTEXT INDEX ").append(_getIndexName(arr)).append(" ")
                            .append(_getIndexFieldList(arr));
                }
                else {
                    if (primary) sql.append("PRIMARY ");
                    else if (unique) sql.append("UNIQUE ");
                    sql.append("KEY ").append(primary ? "" : _getIndexName(arr) + " ")
                            .append(_getIndexFieldList(arr));
                }
            }
        }

        sql.append(")");

//        System.out.println(sql);
        return sql.toString(); // null
    }

    private String _toDBDataType(String dataType) throws Exception {
        switch (dataType) {
            case "boolean": return "tinyint";
            case "integer": return "int";
            case "long": return "bigint";
            case "float": return "float";
            case "double": return "double";
            default:
                if (dataType.startsWith("string")) {
                    int pos = dataType.indexOf('[');
                    int length = (pos < 0) ?
                            stringDefaultLength :
                            Integer.parseInt(dataType.substring(pos+1,dataType.length()-1));
                    if (length >= longStringMinLength) return "text";
                    else return "varchar(" + length + ")";
                }
                else
                    throw new Exception("The data type is not supported: " + dataType);
        }
    }

    private String _getIndexName(JSONArray fields) {
        StringBuilder result = new StringBuilder();
        result.append("idx");

        if (fields!=null && fields.size()>0) {
            for (int i = 0; i < fields.size(); i++) {
                result.append("_").append(fields.getString(i).toLowerCase());
            }
        }

        return result.toString();
    }

    private String _getIndexFieldList(JSONArray fields) {
        StringBuilder result = new StringBuilder();
        result.append("(");
        boolean isFirst = true;

        if (fields!=null && fields.size()>0) {
            for (int i = 0; i < fields.size(); i++) {
                if (isFirst) isFirst = false;
                else result.append(",");
                result.append("`").append(fields.getString(i)).append("`");
            }
        }

        result.append(")");
        return result.toString();
    }

    public String generateAlterTableStatement(String tablename, String dbSchema, String newSchema) throws Exception {
        if (isMemoryDB) {
            // Remove the table and create a new one

            Connection conn = SQLDBStorageImpl.getConnection();
            try {
                try (Statement statement=conn.createStatement()) {
                    statement.execute("DROP TABLE " + tablename);
                }
            } finally {
                SQLDBStorageImpl.closeConnection(conn);
            }

            return generateCreateTableStatement(tablename, newSchema);
        }

        StringBuilder sql = new StringBuilder();
        boolean isFirst = true;

        String updates = _getUpdatesFromSchemas(dbSchema, newSchema);

        if (updates!=null && updates.length()>0) {
            JSONArray ops = JSON.parseArray(updates);
            if (ops.size() > 0) {
                sql.append("ALTER TABLE ").append(tablename).append(" ");

                // Update columns firstly

                for (int i = 0; i < ops.size(); i++) {
                    JSONObject op = ops.getJSONObject(i);
                    String strOp = op.getString("op");
                    String strName = op.getString("name");

                    if (strOp.equals("dropField")) {
                        if (isFirst) isFirst = false;
                        else sql.append(", ");
                        sql.append("DROP COLUMN ").append("`").append(strName).append("`");
                    }
                    else {
                        if (strOp.equals("addField")) {
                            if (isFirst) isFirst = false;
                            else sql.append(", ");
                            sql.append("ADD COLUMN ");
                        }
                        else if (strOp.equals("changeField")) {
                            if (isFirst) isFirst = false;
                            else sql.append(", ");
                            sql.append("CHANGE COLUMN ").append("`").append(strName).append("`").append(" ");
                        }
                        else strOp = null;

                        if (strOp != null) {
                            String dataType = op.getString("type").toLowerCase();
                            sql.append("`").append(strName).append("`").append(" ").append(_toDBDataType(dataType));

                            if (op.getBoolean("null")) sql.append(" DEFAULT NULL");
                            else sql.append(" NOT NULL");
                        }
                    }
                }

                // Then the indexes

                for (int i = 0; i < ops.size(); i++) {
                    JSONObject op = ops.getJSONObject(i);
                    String strOp = op.getString("op");
                    String strName = op.getString("name");

                    if (strOp.equals("dropIndex") || strOp.equals("changeIndex")) { // If change index, first drop then add
                        if (isFirst) isFirst = false;
                        else sql.append(", ");
                        if (strName.equals("PRIMARY")) sql.append("DROP PRIMARY KEY");
                        else sql.append("DROP INDEX ").append(strName);
                    }

                    if (strOp.equals("addIndex") || strOp.equals("changeIndex")) {
                        if (isFirst) isFirst = false;
                        else sql.append(", ");
                        boolean primary = op.getBooleanValue("primary", false);
                        boolean unique = op.getBooleanValue("unique", false);
                        boolean fulltext = op.getBooleanValue("fulltext", false);

                        JSONArray arr = op.containsKey("fields") ?
                                op.getJSONArray("fields") :
                                new JSONArray(op.getString("field"));

                        if (fulltext) {
                            sql.append("ADD FULLTEXT INDEX ").append(strName).append(" ")
                                    .append(_getIndexFieldList(arr));
                        }
                        else {
                            sql.append("ADD ");
                            if (primary) sql.append("PRIMARY ");
                            else if (unique) sql.append("UNIQUE ");
                            sql.append("KEY ").append(primary ? "" : strName + " ")
                                    .append(_getIndexFieldList(arr));
                        }
                    }
                }
            }
        }

//        System.out.println(updates);
//        System.out.println(sql);
        return sql.toString(); // null;
    }

    /**
     Updates JSON:
     [
         {op:"addField", name:"...", type:"...", null:Boolean},
         {op:"dropField", name:"..."},
         {op:"changeField", ...同addField...},

         {op:"addIndex", name:"...", field:"...", fields:["..."], primary:Boolean, unique:Boolean, fulltext:Boolean},
         {op:"dropIndex", ...同addIndex...},
         {op:"changeIndex", ...同addIndex...}
     ]

     Note: If there's a newly promoted primary column, change it to NOT NULL, and merge it with other operations on the same columns
     */
    private String _getUpdatesFromSchemas(String dbSchemaStr, String newSchemaStr) throws Exception {
        StringBuilder result = new StringBuilder();
        result.append("[");

        // Translate Schema into Key-Value (JSONObject) to facilitate the comparison

        JSONObject objDBFields; // fieldName -> fieldType
        JSONObject objNewFields; // fieldName -> fieldType
        JSONObject objDBIndexes; // indexName -> indexObject
        JSONObject objNewIndexes; // indexName -> indexObject
        boolean isFirst = true;

        if (dbSchemaStr!=null && dbSchemaStr.length()>0) {
            JSONObject objDBSchema = JSON.parseObject(dbSchemaStr);

            objDBFields = objDBSchema.getJSONObject("fields");
            if (objDBFields == null) objDBFields = new JSONObject();

            objDBIndexes = _indexesArraryToObject(objDBSchema.getJSONArray("indexes"));
        } else {
            objDBFields = new JSONObject();
            objDBIndexes = new JSONObject();
        }

        if (newSchemaStr!=null && newSchemaStr.length()>0) {
            JSONObject objNewSchema = JSON.parseObject(newSchemaStr);

            objNewFields = objNewSchema.getJSONObject("fields");
            if (objNewFields == null) objNewFields = new JSONObject();

            objNewIndexes = _indexesArraryToObject(objNewSchema.getJSONArray("indexes"));
        } else {
            objNewFields = new JSONObject();
            objNewIndexes = new JSONObject();
        }

        // Output the differences between indexes

        Set<String> dbIndexes = objDBIndexes.keySet();
        Set<String> newIndexes = objNewIndexes.keySet();

        for (String index : newIndexes) {
            JSONObject objIndex = objNewIndexes.getJSONObject(index);
            boolean hasChanges = false;
            if (objDBIndexes.containsKey(index)) {
                // Indexes to be updated
                if (! _compareIndexes(objDBIndexes.getJSONObject(index), objNewIndexes.getJSONObject(index))) {
                    objIndex.put("op", "changeIndex");
                    hasChanges = true;
                }
            }
            else {
                // Indexes to be created
                objIndex.put("op","addIndex");
                hasChanges = true;
            }

            if (hasChanges) {
                if (isFirst) isFirst = false;
                else result.append(", ");
                result.append(objIndex.toJSONString());
            }
        }

        for (String index : dbIndexes) {
            if (! objNewIndexes.containsKey(index)) {
                // Indexes to be dropped
                if (isFirst) isFirst = false;
                else result.append(", ");
                JSONObject objIndex = objDBIndexes.getJSONObject(index);
                objIndex.put("op","dropIndex");
                result.append(objIndex.toJSONString());
            }
        }

        // Find the newly promoted primary columns

        Set<String> diffPrimaryFields = new HashSet<>(); // The new primary columns
        Set<String> dbPrimaryFields = new HashSet<>();
        Set<String> newPrimaryFields = new HashSet<>();

        for (String index : dbIndexes) {
            JSONObject objIndex = objDBIndexes.getJSONObject(index);
            if (objIndex.getBooleanValue("primary", false)) {
                if (objIndex.containsKey("field")) dbPrimaryFields.add(objIndex.getString("field").toLowerCase());
                else _addToSet(dbPrimaryFields, objIndex.getJSONArray("fields"));
            }
        }

        for (String index : newIndexes) {
            JSONObject objIndex = objNewIndexes.getJSONObject(index);
            if (objIndex.getBooleanValue("primary", false)) {
                if (objIndex.containsKey("field")) newPrimaryFields.add(objIndex.getString("field").toLowerCase());
                else _addToSet(newPrimaryFields, objIndex.getJSONArray("fields"));
            }
        }

        diffPrimaryFields.addAll(newPrimaryFields);
        diffPrimaryFields.removeAll(dbPrimaryFields);

        // Output the differences between columns

        Set<String> dbFields = objDBFields.keySet();
        Set<String> newFields = objNewFields.keySet();

        for (String field : newFields) {
            boolean hasChanges = false;
            String objDBField = _getKeyIgnoreCase(dbFields, field);
            if (objDBField != null) {
                // Columns to be updated

                if (diffPrimaryFields.contains(field.toLowerCase()))  {
                    // The new primary columns must be updated, at least changing to NOT NULL

                    if (isFirst) isFirst = false;
                    else result.append(", ");
                    result.append("{\"op\":\"changeField\",");
                    hasChanges = true;
                }
                else {
                    String dbType = objDBFields.getString(objDBField).toLowerCase();
                    String newType = objNewFields.getString(field).toLowerCase();

                    if (!dbType.equals(newType)) {
                        int dbTypeLength = 0;
                        int newTypeLength = 1;

                        if (dbType.startsWith("string") && newType.startsWith("string")) {
                            int dbPos = dbType.indexOf('[');
                            dbTypeLength = (dbPos < 0) ?
                                    stringDefaultLength :
                                    Integer.parseInt(dbType.substring(dbPos + 1, dbType.length() - 1));
                            if (dbTypeLength >= longStringMinLength) dbTypeLength = longStringMinLength;

                            int newPos = newType.indexOf('[');
                            newTypeLength = (newPos < 0) ?
                                    stringDefaultLength :
                                    Integer.parseInt(newType.substring(newPos + 1, newType.length() - 1));
                            if (newTypeLength >= longStringMinLength) newTypeLength = longStringMinLength;
                        }

                        if (dbTypeLength != newTypeLength) {
                            if (isFirst) isFirst = false;
                            else result.append(", ");
                            result.append("{\"op\":\"changeField\",");
                            hasChanges = true;
                        }
                    }
                }
            }
            else {
                // Columns to be created
                if (isFirst) isFirst = false;
                else result.append(", ");
                result.append("{\"op\":\"addField\",");
                hasChanges = true;
            }

            if (hasChanges) {
                result.append("\"name\":\"")
                        .append(field).append("\",\"type\":\"")
                        .append(objNewFields.getString(field)).append("\"")
                        .append(",\"null\":")
                        .append(newPrimaryFields.contains(field.toLowerCase()) ? false : true)
                        .append("}");
            }
        }

        for (String field : dbFields) {
            if (_getKeyIgnoreCase(newFields, field) == null) {
                // Columns to be dropped
                if (isFirst) isFirst = false;
                else result.append(", ");
                result.append("{\"op\":\"dropField\",\"name\":\"").append(field).append("\"}");
            }
        }

//        System.out.println("objDBFields: "+objDBFields.toJSONString());
//        System.out.println("objDBIndexes: "+objDBIndexes.toJSONString());
//        System.out.println("objNewFields: "+objNewFields.toJSONString());
//        System.out.println("objNewIndexes: "+objNewIndexes.toJSONString());

        result.append("]");

//        System.out.println(result.toString());
        return result.toString();
    }

    private String _getKeyIgnoreCase(Set<String> set, String key) {
        for (String str : set) {
            if (str.equalsIgnoreCase(key)) return str;
        }
        return null;
    }

    private JSONObject _indexesArraryToObject(JSONArray arrIndexes) {
        JSONObject result = new JSONObject();

        if (arrIndexes!=null && arrIndexes.size()>0) {
            for (int i = 0; i <arrIndexes.size(); i++) {
                JSONObject index = arrIndexes.getJSONObject(i);
                JSONArray arr = index.containsKey("fields") ?
                        index.getJSONArray("fields") :
                        new JSONArray(index.getString("field"));

                String indexName = index.getBooleanValue("primary", false) ? "PRIMARY" : _getIndexName(arr);

                index.put("name", indexName);
                result.put(indexName, index);
            }
        }

        return result;
    }

    private void _addToSet(Set<String> set, JSONArray arr) {
        for (int i = 0; i < arr.size(); i++) {
            set.add(arr.getString(i).toLowerCase());
        }
    }

    private boolean _compareIndexes(JSONObject index1, JSONObject index2) {
        String field1 = index1.getString("field");
        String field2 = index2.getString("field");
        if (field1==null && field2!=null || field1!=null && field2==null) return false;
        if (field1!=null && field2!=null && ! field1.equalsIgnoreCase(field2)) return false;

        JSONArray fields1 = index1.getJSONArray("fields");
        JSONArray fields2 = index2.getJSONArray("fields");
        if (fields1==null && fields2!=null || fields1!=null && fields2==null) return false;
        if (fields1!=null && fields2!=null) {
            if (fields1.size() != fields2.size()) return false;

            // If the index name is different, the columns (or their orders) must be changed
            if (! _getIndexName(fields1).equals(_getIndexName(fields2))) return false;
        }

        boolean isPrimary1 = index1.getBooleanValue("primary", false);
        boolean isPrimary2 = index2.getBooleanValue("primary", false);
        if (isPrimary1 != isPrimary2) return false;

        if (! isPrimary1) { // If primary is ture, unique is ignored
            boolean isUnique1 = index1.getBooleanValue("unique", false);
            boolean isUnique2 = index2.getBooleanValue("unique", false);
            if (isUnique1 != isUnique2) return false;
        }

        boolean isFulltext1 = index1.getBooleanValue("fulltext", false);
        boolean isFulltext2 = index2.getBooleanValue("fulltext", false);
        if (isFulltext1 != isFulltext2) return false;

        return true;
    }

}
