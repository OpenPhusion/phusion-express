package cloud.phusion.test;

import cloud.phusion.Context;
import cloud.phusion.DataObject;
import cloud.phusion.Engine;
import cloud.phusion.EngineFactory;
import cloud.phusion.express.service.ApplicationService;
import cloud.phusion.express.service.IntegrationService;
import com.alibaba.fastjson2.JSON;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Properties;

public class IntegrationServiceTest {
    private static Context ctx;

    @BeforeClass
    public static void setUp() throws Exception {
        String base = ApplicationServiceTest.class.getClassLoader().getResource("").getPath();
        base = base.replace("target/test-classes/", "storage/");

        Properties props = new Properties();

        props.setProperty(EngineFactory.FileStorage_PrivateRootPath, base+"private");
        props.setProperty(EngineFactory.Module_JavaFileRootPath, base+"private/phusion/jar/");
        props.setProperty(EngineFactory.Module_JavaScriptFileRootPath, base+"private/phusion/code/");

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

        engine.start(ctx);
    }

    @Test
    public void testSave() throws Exception {
//        String str = "{\n" +
//                "    \"id\": \"IT1001\",\n" +
//                "    \"title\": \"IT-1001\",\n" +
//                "    \"clientId\": \"luyao\",\n" +
//                "    \"autoStart\": true,\n" +
//                "    \"applications\": [{\"id\":\"eco\",\"connectionId\":\"conn1001\"}],\n" +
//                "    \"categories\": [\"aaa\"],\n" +
//                "\n" +
//                "    \"timer\": {\"interval\": 3},\n" +
//                "    \"workflow\": [\n" +
//                "        {\n" +
//                "            \"id\": \"01\",\n" +
//                "            \"type\": \"direct\",\n" +
//                "            \"msg\": {\"text\": \"hello\"}\n" +
//                "        },\n" +
//                "        {\n" +
//                "            \"id\": \"02\",\n" +
//                "            \"type\": \"processor\",\n" +
//                "            \"subtype\": \"javascript\",\n" +
//                "            \"from\": \"01\"\n" +
//                "        },\n" +
//                "        {\n" +
//                "            \"id\": \"03\",\n" +
//                "            \"type\": \"processor\",\n" +
//                "            \"subtype\": \"java\",\n" +
//                "            \"module\": \"test\",\n" +
//                "            \"class\": \"cloud.phusion.test.SimpleProcessor\",\n" +
//                "            \"from\": \"02\"\n" +
//                "        },\n" +
//                "        {\n" +
//                "            \"id\": \"exception\",\n" +
//                "            \"type\": \"processor\",\n" +
//                "            \"subtype\": \"javascript\"\n" +
//                "        }\n" +
//                "    ]\n" +
//                "}";
//        IntegrationService.saveIntegration("IT1001", new DataObject(str), true, true, true, false, ctx);
    }

    @Test
    public void testFetch() throws Exception {
//        DataObject result = IntegrationService.fetchIntegration("IT1001", ctx);
//        System.out.println( result==null ? null : result.getString() );
    }

    @Test
    public void testGenerateScript() throws Exception {
//        IntegrationService.generateScriptFile("IT1001", null, ctx);
    }

    @Test
    public void testTemplates() throws Exception {
//        DataObject result = IntegrationService.listTemplates(ctx);
//        System.out.println( result==null ? null : result.getString() );
    }

    @Test
    public void testListById() throws Exception {
//        DataObject result = IntegrationService.listIntegrationsById("IT1001,IT1002,IT10001","id,status,template",ctx);
//        System.out.println( result==null ? null : result.getString() );
    }

    @Test
    public void testIsTemplate() throws Exception {
//        boolean result = IntegrationService.isTemplate("IT1001", ctx);
//        System.out.println( result );
    }

    @Test
    public void testQuery() throws Exception {
//        DataObject result = IntegrationService.queryIntegrations(null,"id,applications",0,100,ctx);
//        System.out.println( result==null ? null : result.getString() );
    }

    @Test
    public void testCount() throws Exception {
//        long result = IntegrationService.countIntegrations(null,ctx);
//        System.out.println( result );
    }

    @Test
    public void testRemove() throws Exception {
//        IntegrationService.removeIntegration("IT1001", ctx);
    }

    @Test
    public void testUpdateConfig() throws Exception {
//        IntegrationService.startIntegration("IT1001", ctx);
//
//        Thread.sleep(5000);
//
//        IntegrationService.dynamicUpdateStepMessages("IT1001", ctx);
    }

    @Test
    public void testSaveStepMsg() throws Exception {
//        String str = "{\"01\":{\"command\":\"open\"}}";
//        IntegrationService.saveStepMessages("IT1001", JSON.parseObject(str), ctx);
    }

    @Test
    public void testSaveConfig() throws Exception {
//        String str = "{\"text\":{\"hello\":\"world\"}}";
//        IntegrationService.saveConfig("IT1001", JSON.parseObject(str), ctx);
    }

    @Test
    public void testRestart() throws Exception {
//        IntegrationService.restartIntegration("IT1002", ctx);
//
//        Thread.sleep(5000);
    }

    @Test
    public void testStartAll() throws Exception {
//        IntegrationService.startAllIntegrations(ctx);
    }

    @Test
    public void testTest() throws Exception {
//        DataObject msg = new DataObject("{\"name\":\"world\"}");
//        DataObject props = new DataObject("{\"score\":123}");
//
//        DataObject result = IntegrationService.testIntegration(
//                "IT1001", "01", false, msg, props, false, ctx
//        );
//        System.out.println( result==null ? null : result.getString() );
    }

    @AfterClass
    public static void tearDown() throws Exception {

    }

}
