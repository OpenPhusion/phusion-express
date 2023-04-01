package cloud.phusion.test;

import cloud.phusion.Context;
import cloud.phusion.DataObject;
import cloud.phusion.Engine;
import cloud.phusion.EngineFactory;
import cloud.phusion.express.service.TransactionService;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class TransactionServiceTest {
    private static Context ctx;

    @BeforeClass
    public static void setUp() throws Exception {
        Properties props = new Properties();

//        props.setProperty(EngineFactory.DB_Type, EngineFactory.DBType_JDBC);
//        props.setProperty(EngineFactory.JDBC_DriverClass, "com.mysql.cj.jdbc.Driver");
//        props.setProperty(EngineFactory.JDBC_Url, "jdbc:mysql://8.8.8.8:4729/phusion");
//        props.setProperty(EngineFactory.JDBC_DBName, "phusion");
//        props.setProperty(EngineFactory.JDBC_User, "phusion");
//        props.setProperty(EngineFactory.JDBC_Password, "123456");
//
//        props.setProperty(EngineFactory.Redis_Host, "192.168.30.158");
//        props.setProperty(EngineFactory.Redis_Port, "6379");
//        props.setProperty(EngineFactory.Redis_Database, "0");
//        props.setProperty(EngineFactory.Redis_Auth, "123");
//
//        props.setProperty(EngineFactory.Scheduler_Enabled, "false");
//        props.setProperty(EngineFactory.Scheduler_Clustered, "true");
//        props.setProperty(EngineFactory.Scheduler_RandomRange, "500");
//        props.setProperty(EngineFactory.Scheduler_LockTime, "2000");

        Engine engine = EngineFactory.createEngine(props);
        ctx = EngineFactory.createContext(engine);

//        engine.start(ctx);
    }

    @Test
    public void testCalculateStats() throws Exception {
//        TransactionService.calculateStats(ctx);
    }

    @Test
    public void testStepStats() throws Exception {
//        DataObject obj = TransactionService.getTransactionStepStats(
//                "IT10001", null, null, ctx
//        );
//        if (obj != null) System.out.println(obj.getString());
    }

    @Test
    public void testFetchTransaction() throws Exception {
//        DataObject obj = TransactionService.fetchTransaction("1001", ctx);
//        if (obj != null) System.out.println(obj.getString());
    }

    @Test
    public void testListTransactionsById() throws Exception {
//        DataObject obj = TransactionService.listTransactionsById("1001", null, ctx);
//        if (obj != null) System.out.println(obj.getString());
    }

    @Test
    public void testListTransactions() throws Exception {
//        List<String> whereFields = new ArrayList<>();
//        whereFields.add("search");
//        List<Object> params = new ArrayList<>();
//        params.add("A80001");
//
//        DataObject obj = TransactionService.listTransactions(null, whereFields, params, 0, 1000, ctx);
//        if (obj != null) System.out.println(obj.getString());
    }

    @Test
    public void testStatsTransactions() throws Exception {
//        List<String> whereFields = new ArrayList<>();
//        whereFields.add("integrationId");
//        List<Object> params = new ArrayList<>();
//        params.add("IT10001");
//
//        DataObject obj = TransactionService.getTransactionStats(whereFields, params, ctx);
//        if (obj != null) System.out.println(obj.getString());
    }

    @Test
    public void testGroupStatsTransactions() throws Exception {
//        List<String> whereFields = new ArrayList<>();
//        List<Object> params = new ArrayList<>();
//
//        DataObject obj = TransactionService.getTransactionGroupStats(whereFields, params, "hourOnly", ctx);
//        if (obj != null) System.out.println(obj.getString());
    }

    @AfterClass
    public static void tearDown() throws Exception {

    }

}
