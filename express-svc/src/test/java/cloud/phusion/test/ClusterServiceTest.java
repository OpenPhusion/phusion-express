package cloud.phusion.test;

import cloud.phusion.*;
import cloud.phusion.express.controller.ClusterController;
import cloud.phusion.express.service.ClusterService;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Properties;

public class ClusterServiceTest {
    private static Context ctx;

    @BeforeClass
    public static void setUp() throws Exception {
//        Properties props = new Properties();
//
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
//        props.setProperty(EngineFactory.Scheduler_Enabled, "true");
//        props.setProperty(EngineFactory.Scheduler_Clustered, "true");
//        props.setProperty(EngineFactory.Scheduler_RandomRange, "500");
//        props.setProperty(EngineFactory.Scheduler_LockTime, "2000");
//
//        Engine engine = EngineFactory.createEngine(props);
//        ctx = EngineFactory.createContext(engine);
//
//        engine.start(ctx);
    }

    @Test
    public void testHeartbeat() throws Exception {
//        ClusterService.init("2", ctx);
//        Thread.sleep(5000);
    }

    @Test
    public void testListEngines() throws Exception {
//        ClusterService.init("10", ctx);
//        Thread.sleep(1000);
//
//        System.out.println( ClusterService.listEngines(ctx).getString() );
    }

    @Test
    public void testListObjects() throws Exception {
//        ClusterService.init("10", ctx);
//        Thread.sleep(1000);
//
//        System.out.println( ClusterService.listObjectStatus(
//                ClusterService.OBJECT_APPLICATION, "eco",
//                ctx).getString() );
    }

    @Test
    public void testRemoveObject() throws Exception {
//        ClusterService.removeObject(ClusterService.OBJECT_APPLICATION, "eco", ctx);
    }

    @AfterClass
    public static void tearDown() throws Exception {

    }

}
