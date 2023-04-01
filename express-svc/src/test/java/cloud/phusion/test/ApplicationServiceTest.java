package cloud.phusion.test;

import cloud.phusion.Context;
import cloud.phusion.DataObject;
import cloud.phusion.Engine;
import cloud.phusion.EngineFactory;
import cloud.phusion.express.service.ApplicationService;
import cloud.phusion.express.service.ClientService;
import cloud.phusion.express.util.CommonCode;
import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONArray;
import com.alibaba.fastjson2.JSONObject;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class ApplicationServiceTest {
    private static Context ctx;

    @BeforeClass
    public static void setUp() throws Exception {
        String base = ApplicationServiceTest.class.getClassLoader().getResource("").getPath();
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

        props.setProperty(EngineFactory.HttpServer_Enabled, "true");

        props.setProperty(EngineFactory.Scheduler_Enabled, "false");
        props.setProperty(EngineFactory.Scheduler_Clustered, "false");
        props.setProperty(EngineFactory.Scheduler_RandomRange, "500");
        props.setProperty(EngineFactory.Scheduler_LockTime, "2000");

        Engine engine = EngineFactory.createEngine(props);
        ctx = EngineFactory.createContext(engine);
    }

    @Test
    public void testHasApplication() throws Exception {
//        boolean result = ApplicationService.hasApplication("eco", ctx);
//        System.out.println(result ? "Yes" : "No");
    }

    @Test
    public void testFetchApplication() throws Exception {
//        DataObject app = ApplicationService.fetchApplication("eco", ctx);
//        if (app != null) System.out.println(app.getJSONObject().toJSONString());
    }

    @Test
    public void testSaveApplication() throws Exception {
//        String id = "eco";
//        String str = "{\"id\":\""+id+"\",\"desc\":\"Demo application\"}";
//
//        ApplicationService.saveApplication(id, new DataObject(str), false, false, ctx);
//
//        DataObject app = ApplicationService.fetchApplication(id, ctx);
//        if (app != null) System.out.println(app.getJSONObject().toJSONString());
    }

    @Test
    public void testQueryApplication() throws Exception {
//        DataObject result = ApplicationService.queryApplications(null, "desc,status,autoStart", 0, 100, ctx);
//        if (result != null) System.out.println(result.getString());
    }

    @Test
    public void testListProtocols() throws Exception {
//        DataObject result = ApplicationService.listProtocols(ctx);
//        if (result != null) System.out.println(result.getString());
    }

    @Test
    public void testRemoveApplication() throws Exception {
        String id = "eco-new";
//        ApplicationService.removeApplication(id, ctx);
    }

    @Test
    public void testStartApplication() throws Exception {
//        ctx.getEngine().start(ctx);
//        ApplicationService.startApplication("eco", ctx);
    }

    @Test
    public void testStartAllApplications() throws Exception {
//        ctx.getEngine().start(ctx);
//        ApplicationService.startAllApplications(ctx);
//        ApplicationService.restartApplication("xcharge",ctx);
    }

    @Test
    public void testExpandApplications() throws Exception {
//        JSONArray arr = new JSONArray();
//        JSONObject item = new JSONObject();
//        item.put("id", "eco2222");
//        item.put("connId", "conn2002");
//        arr.add(item);
//        item = new JSONObject();
//        item.put("id", "eco1111");
//        item.put("connId", "conn1001");
//        arr.add(item);
//
//        Map<String, Object> obj = CommonCode.convertJSONArrayToMap(arr, "id");
//        ApplicationService.expandApplicationProtocols(obj, ctx);
//
//        for (String key : obj.keySet()) {
//            System.out.println(key+": "+((JSONObject)obj.get(key)).toJSONString());
//        }
    }

    @AfterClass
    public static void tearDown() throws Exception {

    }

}
