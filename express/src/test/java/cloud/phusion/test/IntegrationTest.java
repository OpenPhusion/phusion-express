package cloud.phusion.test;

import cloud.phusion.*;
import org.junit.*;

public class IntegrationTest {
    private static Engine engine = null;
    private static Context ctx = null;
    private static String basePath = null;
    private static String itId = "TestIT";

    @BeforeClass
    public static void setUp() throws Exception {
//        Properties props = new Properties();
//
////        props.setProperty(EngineFactory.Scheduler_Enabled, "true");
////        props.setProperty(EngineFactory.HttpServer_Enabled, "true");
//
//        props.setProperty(EngineFactory.DB_Type, EngineFactory.DBType_JDBC);
//        props.setProperty(EngineFactory.JDBC_DriverClass, "com.mysql.cj.jdbc.Driver");
//        props.setProperty(EngineFactory.JDBC_Url, "jdbc:mysql://192.168.1.1:1111/db");
//        props.setProperty(EngineFactory.JDBC_DBName, "db");
//        props.setProperty(EngineFactory.JDBC_User, "user");
//        props.setProperty(EngineFactory.JDBC_Password, "123456");
//        props.setProperty(EngineFactory.TRXLog_Target, "phusion");
//        props.setProperty(EngineFactory.TRXLog_EncodeMsg, "true");
//
//        engine = EngineFactory.createEngine(props);
//        ctx = EngineFactory.createContext(engine);
//
//        engine.start(ctx);
//
//        basePath = IntegrationTest.class.getClassLoader().getResource("").getPath() + "workflow/";
//
////        engine.registerApplication("AppSimple", "cloud.phusion.test.util.SimpleApp", null, ctx);
////        Application app = ctx.getEngine().getApplication("AppSimple");
////        app.start(ctx);
////        app.createConnection("LuyaoChargerConn", null, ctx);
////        app.connect("LuyaoChargerConn", ctx);
    }

    @Test
    public void testIntegrationManagement() throws Exception {
//        IntegrationDefinition idef = new IntegrationDefinition();
//        idef.setWorkflow(basePath + "workflow.json");
//        DataObject config = new DataObject("{}");
//
//        assertEquals( engine.getIntegrationStatus(itId), ExecStatus.None );
//
//        engine.registerIntegration( itId, idef, config, ctx );
//        assertEquals( engine.getIntegrationStatus(itId), ExecStatus.Stopped );
//        Integration it = engine.getIntegration(itId);
//
//        it.start(ctx);
//        assertEquals( engine.getIntegrationStatus(itId), ExecStatus.Running );
//
//        try {
//            engine.removeIntegration(itId, ctx);
//            fail();
//        } catch (Exception ex) {
//            assertTrue( ex instanceof PhusionException );
//        }
//
//        it.stop(ctx);
//        assertEquals( engine.getIntegrationStatus(itId), ExecStatus.Stopped );
//
//        engine.removeIntegration(itId, ctx);
//        assertEquals( engine.getIntegrationStatus(itId), ExecStatus.None );
    }

    @Test
    public void testDirect() throws Exception {
//        IntegrationDefinition idef = new IntegrationDefinition();
//        idef.setWorkflow(basePath + "workflow-01.json");
//        DataObject config = new DataObject("{}");
//
//        engine.registerIntegration(itId, idef, config, ctx);
//        Integration it = engine.getIntegration(itId);
//        it.start(ctx);
    }

    @Test
    public void testInboundEndpoint() throws Exception {
//        IntegrationDefinition idef = new IntegrationDefinition();
//        idef.setWorkflow(basePath + "workflow-02.json");
//        DataObject config = new DataObject("{}");
//
//        engine.registerIntegration(itId, idef, config, ctx);
//        Integration it = engine.getIntegration(itId);
//        it.start(ctx);
//
//        it.stop(ctx);
//        it.updateConfig(new DataObject("{\"data\":10}"), ctx);
//        it.updateStepMsg("02", new DataObject("{\"status\":\"FAILED!\"}"), ctx);
//
//        it.start(ctx);
//
//        HttpClient http = engine.createHttpClient();
//        HttpResponse response = http.post("http://localhost:9900/AppSimple/orders?conn=LuyaoChargerConn")
//                .header("Content-Type", "application/json")
//                .body("{\"a\":111}")
//                .send();
//
//        System.out.println(response.getBody().getString());
    }

    @Test
    public void testExecutionFail() throws Exception {
//        IntegrationDefinition idef = new IntegrationDefinition();
//        idef.setWorkflow(basePath + "workflow-03.json");
//        DataObject config = new DataObject("{}");
//
//        engine.registerIntegration(itId, idef, config, ctx);
//        Integration it = engine.getIntegration(itId);
//        it.start(ctx);
    }

    @Test
    public void testJavaAndJavaScript() throws Exception {
//        IntegrationDefinition idef = new IntegrationDefinition();
//        idef.setWorkflow(basePath + "workflow-04.json");
//        DataObject config = new DataObject("{}");
//
//        engine.loadJavaScriptModule("I1001", basePath+"../javascript/SimpleStep.js", ctx);
//        engine.loadJavaModule("Simple", new String[]{basePath+"../phusion-express-0.1.0-SNAPSHOT.jar"}, ctx);
//
//        engine.registerIntegration(itId, idef, config, ctx);
//        Integration it = engine.getIntegration(itId);
//        it.start(ctx);
    }

    @Test
    public void testProbe() throws Exception {
//        IntegrationDefinition idef = new IntegrationDefinition();
//        idef.setWorkflow(basePath + "workflow-04.json");
//        DataObject config = new DataObject("{}");
//
//        engine.loadJavaModule("Simple", new String[]{basePath+"../phusion-express-0.1.0-SNAPSHOT.jar"}, ctx);
//        engine.registerIntegration(itId, idef, config, ctx);
//        Integration it = engine.getIntegration(itId);
//
//        Transaction trx = it.createInstance(
//                new DataObject("{\"test\":\"hello\"}"),
//                "03", "02", false, null, ctx);
//
//        it.probe(trx);
//
//        System.out.println(trx.toJSONString());
    }

    @Test
    public void testSchedule() throws Exception {
//        IntegrationDefinition idef = new IntegrationDefinition();
//        idef.setWorkflow(basePath + "workflow-01.json");
//        DataObject config = new DataObject("{}");
//
////        idef.setPeriodicSchedule(2,0);
//        idef.setCronSchedule("0/2 * * * * ?");
//
//        engine.registerIntegration(itId, idef, config, ctx);
//        Integration it = engine.getIntegration(itId);
//        it.start(ctx);
//
//        Thread.sleep(10000);
    }

    @Test
    public void testForEach() throws Exception {
//        IntegrationDefinition idef = new IntegrationDefinition();
//        idef.setWorkflow(basePath + "workflow-05.json");
//        DataObject config = new DataObject("{}");
//
//        engine.loadJavaScriptModule("I1002", basePath+"../javascript/CollectStep.js", ctx);
//
//        engine.registerIntegration(itId, idef, config, ctx);
//        Integration it = engine.getIntegration(itId);
//        it.start(ctx);
    }

    @Test
    public void testAsyncJS() throws Exception {
//        IntegrationDefinition idef = new IntegrationDefinition();
//        idef.setWorkflow(basePath + "workflow-06.json");
//        DataObject config = new DataObject("{}");
//
//        engine.loadJavaScriptModule("I1003", basePath+"../javascript/AsyncStep.js", ctx);
//
//        engine.registerIntegration(itId, idef, config, ctx);
//        Integration it = engine.getIntegration(itId);
//        it.start(ctx);
    }

    @AfterClass
    public static void tearDown() {
    }

}
