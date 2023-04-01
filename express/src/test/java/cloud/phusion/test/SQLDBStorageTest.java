package cloud.phusion.test;

import cloud.phusion.Context;
import cloud.phusion.Engine;
import cloud.phusion.EngineFactory;
import cloud.phusion.storage.DBStorage;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class SQLDBStorageTest {
    private static String table = "Order";
    private static DBStorage storage = null;
    private static Context ctx = null;

    @BeforeClass
    public static void setUp() throws Exception {
        Properties props = new Properties();
        props.setProperty(EngineFactory.DB_Type, EngineFactory.DBType_JDBC);

//        props.setProperty(EngineFactory.JDBC_DriverClass, "com.mysql.cj.jdbc.Driver");
//        props.setProperty(EngineFactory.JDBC_Url, "jdbc:mysql://192.168.1.1:1111/db");
//        props.setProperty(EngineFactory.JDBC_DBName, "db");
//        props.setProperty(EngineFactory.JDBC_User, "user");
//        props.setProperty(EngineFactory.JDBC_Password, "123456");

        props.setProperty(EngineFactory.JDBC_DriverClass, "org.h2.Driver");
        props.setProperty(EngineFactory.JDBC_Url, "jdbc:h2:mem:test;DATABASE_TO_UPPER=FALSE");

        Engine engine = EngineFactory.createEngine(props);
        ctx = EngineFactory.createContext(engine);
        engine.start(null);

        storage = engine.getDBStorageForClient("XCH");
    }

    @Test
    public void test01CreateTable() throws Exception {
//        String schema1 = "{" +
//                "\"fields\":" +
//                "{\"id\":\"Long\",\"name\":\"String\"," +
//                "\"score\":\"Double\",\"male\":\"Boolean\"," +
//                "\"time\":\"String[45]\",\"x\":\"String[65535]\"}," +
//                "\"indexes\":[" +
//                "{\"field\":\"id\",\"primary\":true,\"unique\":true}," +
//                "{\"fields\":[\"name\",\"score\"],\"primary\":false,\"unique\":true}," +
//                "{\"field\":\"time\",\"primary\":false,\"unique\":false}," +
//                "{\"field\":\"x\",\"fulltext\":true}]}";
//
//        String schema2 = "{" +
//                "\"fields\":" +
//                "{\"id\":\"Long\",\"name\":\"String[45]\"," +
//                "\"score\":\"Double\",\"male\":\"Boolean\"," +
//                "\"y\":\"String[20000]\"," +
//                "\"x\":\"String\"}," +
//                "\"indexes\":[" +
//                "{\"field\":\"id\",\"primary\":false,\"unique\":true}," +
//                "{\"fields\":[\"name\",\"score\"],\"primary\":true,\"unique\":true}]}";
//
//        storage.prepareTable(table, schema1);
    }

    @Test
    public void test02Insert() throws Exception {
//        int i = 0;
//        i += storage.insertRecord(table,
//                new Record("{\"id\":\"1001\", \"name\":\"Alice\", \"score\":82, \"male\":0, \"time\":\"2022-07-01 00:03:04\"}")
//        );
//        i += storage.insertRecord(table,
//                new Record("{\"id\":\"1002\", \"name\":\"Bob\", \"score\":90, \"male\":1, \"time\":\"2022-07-02 00:03:04\"}")
//        );
//        i += storage.insertRecord(table,
//                new Record("{\"id\":\"1003\", \"name\":\"Catherine\", \"score\":60, \"male\":0, \"time\":\"2022-07-03 00:03:04\"}")
//        );
//
//        System.out.println(i);
    }

    @Test
    public void test03Upsert() throws Exception {
//        List<Object> params = new ArrayList<>();
//        params.add("1001");
//
//        int i = 0;
//        i += storage.upsertRecord(table,
//                "id=?", params,
//                new Record("{\"id\":\"1001\", \"name\":\"Alice\", \"score\":82, \"male\":0, \"time\":\"2022-07-01 00:03:04\"}")
//        );
//
//        System.out.println(i);
    }

    @Test
    public void test04Count() throws Exception {
//        List<Object> params = new ArrayList<Object>();
//        params.add(new Integer(60));
//        params.add(new Integer(90));
//
//        long result = storage.queryCount(table, "male", "score between ? and ?", params);
//        System.out.println(result);
    }

    @Test
    public void test05Query() throws Exception {
//        List<Object> params = new ArrayList<Object>();
//        params.add(50);
//        params.add(10);
//
//        Record[] records = storage.queryRecords(table,
//                "name, concat('\"',male,'\"') as m, sum(score) as s", // SELECT
//                 "score>=?", // WHERE
//                "name, male", // GROUP BY
//                "s > ?", // HAVING
//                params, // WHERE parameters
//                "s", // ORDER BY
//                0,
//                0
//        );
//
//        if (records != null) {
//            for (int i = 0; i < records.length; i++) {
//                System.out.println(records[i].toJSONString());
//            }
//        }
    }

    @Test
    public void test06Update() throws Exception {
//        ArrayList<Object> arr = new ArrayList<>();
//        arr.add(new Integer(1003));
//
//        int i = storage.updateRecords(table,
//                new Record("{\"name\":\"Alice2\", \"score\":90.5, \"male\":null}"),
//                "id=?",
//                arr
//        );
//
//        System.out.println(i);
    }

    @Test
    public void test07FreeUpdate() throws Exception {
//        List<Object> params = new ArrayList<>();
//        params.add(120.0);
//        params.add(1001);
//        System.out.println( storage.freeUpdate("update CXCH_Order set score=? where id=?", params, ctx) );
    }

    @Test
    public void test08Delete() throws Exception {
//        String sql = "name like ?";
//
//        ArrayList<Object> params = new ArrayList<>();
//        params.add("A%");
//
//        int i = storage.deleteRecords(table, sql, params);
//
//        System.out.println(i);
    }

    @Test
    public void test09DeleteTable() throws Exception {
//        if (storage.doesTableExist(table)) {
//            storage.removeTable(table);
//        }
//
//        assertFalse(storage.doesTableExist(table));
    }

    @AfterClass
    public static void tearDown() throws Exception {
        ctx.getEngine().stop(null);
    }
}
