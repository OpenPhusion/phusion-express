package cloud.phusion.test;

import cloud.phusion.Context;
import cloud.phusion.DataObject;
import cloud.phusion.Engine;
import cloud.phusion.EngineFactory;
import cloud.phusion.express.service.ApplicationService;
import cloud.phusion.express.service.ClientService;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Properties;

public class ClientServiceTest {
    private static Context ctx;

    @BeforeClass
    public static void setUp() throws Exception {
        String base = ClientServiceTest.class.getClassLoader().getResource("").getPath();
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

        props.setProperty(EngineFactory.Scheduler_Enabled, "false");
        props.setProperty(EngineFactory.Scheduler_Clustered, "false");
        props.setProperty(EngineFactory.Scheduler_RandomRange, "500");
        props.setProperty(EngineFactory.Scheduler_LockTime, "2000");

        Engine engine = EngineFactory.createEngine(props);
        ctx = EngineFactory.createContext(engine);
    }

    @Test
    public void testHasClient() throws Exception {
//        boolean result = ClientService.hasClient("luyao", ctx);
//        System.out.println(result ? "Yes" : "No");
    }

    @Test
    public void testFetchClient() throws Exception {
//        DataObject client = ClientService.fetchClient("luyao", ctx);
//        if (client != null) System.out.println(client.getJSONObject().toJSONString());
    }

    @Test
    public void testSaveClient() throws Exception {
//        String id = "luyao";
//        String str = "{\"id\":\""+id+"\",\"desc\":\"Demo client\",\"title\":\"Lu Yao\"}";
//
//        ClientService.saveClient(id, new DataObject(str), false, null, ctx);
//
//        DataObject client = ClientService.fetchClient(id, ctx);
//        if (client != null) System.out.println(client.getJSONObject().toJSONString());
    }

    @Test
    public void testQueryClients() throws Exception {
//        DataObject result = ClientService.queryClients(null, "id", 0, 100, ctx);
//        if (result != null) System.out.println(result.getString());
    }

    @Test
    public void testRemoveClient() throws Exception {
        String id = "luyao";
//        ClientService.removeClient(id, ctx);
    }

    @AfterClass
    public static void tearDown() throws Exception {

    }

}
