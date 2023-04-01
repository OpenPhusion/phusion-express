package cloud.phusion.test;

import cloud.phusion.Context;
import cloud.phusion.DataObject;
import cloud.phusion.Engine;
import cloud.phusion.EngineFactory;
import cloud.phusion.application.ConnectionStatus;
import cloud.phusion.express.service.ApplicationService;
import cloud.phusion.express.service.ConnectionService;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Properties;

public class ConnectionServiceTest {
    private static Context ctx;

    @BeforeClass
    public static void setUp() throws Exception {
        String base = ConnectionServiceTest.class.getClassLoader().getResource("").getPath();
        base = base.replace("target/test-classes/", "storage/");

        Properties props = new Properties();

        props.setProperty(EngineFactory.FileStorage_PrivateRootPath, base+"private");
        props.setProperty(EngineFactory.Module_JavaFileRootPath, base+"private/phusion/jar/");

        props.setProperty(EngineFactory.DB_Type, EngineFactory.DBType_JDBC);
        props.setProperty(EngineFactory.JDBC_DriverClass, "com.mysql.cj.jdbc.Driver");
        props.setProperty(EngineFactory.JDBC_Url, "jdbc:mysql://8.8.8.8:4729/phusion");
        props.setProperty(EngineFactory.JDBC_DBName, "phusion");
        props.setProperty(EngineFactory.JDBC_User, "phusion");
        props.setProperty(EngineFactory.JDBC_Password, "123456");

        props.setProperty(EngineFactory.Redis_Host, "192.168.30.158");
        props.setProperty(EngineFactory.Redis_Port, "6379");
        props.setProperty(EngineFactory.Redis_Database, "0");
        props.setProperty(EngineFactory.Redis_Auth, "123");

        props.setProperty(EngineFactory.HttpServer_Enabled, "false");

        props.setProperty(EngineFactory.Scheduler_Enabled, "false");
        props.setProperty(EngineFactory.Scheduler_Clustered, "false");
        props.setProperty(EngineFactory.Scheduler_RandomRange, "500");
        props.setProperty(EngineFactory.Scheduler_LockTime, "2000");

        Engine engine = EngineFactory.createEngine(props);
        ctx = EngineFactory.createContext(engine);

        engine.start(ctx);
    }

    @Test
    public void testHasConnection() throws Exception {
//        boolean result = ConnectionService.hasConnection("eco-10001", ctx);
//        System.out.println(result ? "Yes" : "No");
    }

    @Test
    public void testGetLocalStatus() throws Exception {
//        String result = ConnectionService.getConnectionLocalStatus("eco-10001", "eco", ctx);
//        System.out.println(result);
    }

    @Test
    public void testFetchConnection() throws Exception {
//        DataObject app = ConnectionService.fetchConnection("eco-10001", ctx);
//        if (app != null) System.out.println(app.getJSONObject().toJSONString());
    }

    @Test
    public void testSaveConnection() throws Exception {
        String id = "eco-10001";
        String str = "{\"id\":\""+id+"\",\"desc\":\"Demo connection\",\"applicationId\":\"eco\",\"clientId\":\"luyao\"," +
                "\"config\":{\"client\":\"luyao\",\"secretKey\":\"mhzc_charge_open_api_001\"}}";

//        ConnectionService.saveConnection(id, new DataObject(str), ctx);
//
//        DataObject app = ConnectionService.fetchConnection(id, ctx);
//        if (app != null) System.out.println(app.getJSONObject().toJSONString());
    }

    @Test
    public void testListConnection() throws Exception {
//        DataObject result = ConnectionService.listConnections("eco", "luyao", "desc,status", ctx);
//        if (result != null) System.out.println(result.getString());
    }

    @Test
    public void testOperateConnection() throws Exception {
//        String id = "eco-10001";
//
//        ApplicationService.startApplication("xcharge", ctx);
//        ConnectionService.startConnection(id, ctx);
//
//        Thread.sleep(5000);
//        ConnectionService.stopConnection(id, ctx);
    }

    @Test
    public void testRestartConnection() throws Exception {
//        String id = "eco-10001";
//
//        ApplicationService.startApplication("xcharge", ctx);
//        ConnectionService.startConnection(id, ctx);
//        ConnectionService.restartConnection(id, ctx);
    }

    @Test
    public void testRemoveConnection() throws Exception {
//        String id = "eco-10001";
//        ConnectionService.removeConnection(id, ctx);
    }

    @Test
    public void testStartConnections() throws Exception {
//        ApplicationService.startApplication("xcharge", ctx);
//        ConnectionService.startConnections("xcharge", ctx);
    }

    @Test
    public void testRemoveConnections() throws Exception {
//        ConnectionService.removeConnections(null, "luyao", ctx);
    }

    @AfterClass
    public static void tearDown() throws Exception {

    }

}
