package cloud.phusion.test;

import cloud.phusion.Context;
import cloud.phusion.Engine;
import cloud.phusion.EngineFactory;
import org.junit.*;

import java.util.Properties;

public class JavaScriptExecutorTest {
    private static Engine engine;
    private static Context ctx;

    @BeforeClass
    public static void setup() throws Exception {
        String base = FileStorageTest.class.getClassLoader().getResource("").getPath();

        Properties props = new Properties();
//        props.setProperty(EngineFactory.Redis_Host, "192.168.1.1");
//        props.setProperty(EngineFactory.Redis_Port, "1111");
//        props.setProperty(EngineFactory.Redis_Database, "0");
//        props.setProperty(EngineFactory.Redis_Auth, "123456");
//
//        props.setProperty(EngineFactory.DB_Type, EngineFactory.DBType_JDBC);
//        props.setProperty(EngineFactory.JDBC_DriverClass, "com.mysql.cj.jdbc.Driver");
//        props.setProperty(EngineFactory.JDBC_Url, "jdbc:mysql://192.168.1.1:1111/db");
//        props.setProperty(EngineFactory.JDBC_DBName, "db");
//        props.setProperty(EngineFactory.JDBC_User, "user");
//        props.setProperty(EngineFactory.JDBC_Password, "123456");
//
//        props.setProperty(EngineFactory.FileStorage_PrivateRootPath, base + "FileStorage/private");
//        props.setProperty(EngineFactory.FileStorage_PublicRootPath, base + "FileStorage/public");
//        props.setProperty(EngineFactory.FileStorage_PublicRootUrl, "https://phusion.cloud/filestorage");

        props.setProperty(EngineFactory.Module_JavaScriptFileRootPath, base + "javascript/");

        engine = EngineFactory.createEngine(props);
        ctx = EngineFactory.createContext(engine);
    }

    @AfterClass
    public static void tearDown() {
    }

    @Test
    public void testParallel() throws Exception {
//        ThreadPoolExecutor pool = new ThreadPoolExecutor(2, 2, 3,
//                TimeUnit.SECONDS, new ArrayBlockingQueue<Runnable>(10),
//                new ThreadPoolExecutor.AbortPolicy());
//
//        JavaScriptExecutor jsmain = new JavaScriptExecutor();
//        jsmain.loadScript("NodeTest", "JSBridgeTest.js", ctx);
//
//        for (int i = 0; i < 10; i++) {
//            pool.execute(() -> {
//                try {
//                    JavaScriptExecutor js = new JavaScriptExecutor();
//                    Transaction trx = new Transaction(null,null,null,ctx);
//                    trx.setMessage(new DataObject("{\"text\":\"Hello\"}"));
//
//                    long t0 = System.nanoTime();
//                    js.runScriptWithinTransaction("NodeTest", trx);
//                    long t1 = System.nanoTime();
//
//                    System.out.println(trx.getMessage().getString() + " in " + ((t1-t0)/100000/10.0) +"ms");
//                } catch (Exception ex) {ex.printStackTrace();}
//            });
//        }
//
//        Thread.sleep(1500);
    }

    @Test
    public void testKVStorage() throws Exception {
//        engine.loadJavaScriptModule("KVStorageTest", "KVStorageTest.js", ctx);
//
//        Transaction trx = new Transaction(null,null,null,ctx);
//        trx.setMessage(new DataObject("{}"));
//
//        long t0 = System.nanoTime();
//        engine.runJavaScriptWithTransaction("KVStorageTest", trx);
//        long t1 = System.nanoTime();
//
//        System.out.println("In " + ((t1-t0)/100000/10.0) +"ms");
    }

    @Test
    public void testDBStorage() throws Exception {
        // Need database table: CXCH_Order !!!!!!!

//        engine.loadJavaScriptModule("DBStorageTest", "DBStorageTest.js", ctx);
//
//        Transaction trx = new Transaction("I1001","120001","01", ctx);
//        trx.setClientId("XCH");
//        trx.setMessage(new DataObject("{}"));
//
//        long t0 = System.nanoTime();
//        engine.runJavaScriptWithTransaction("DBStorageTest", trx);
//        long t1 = System.nanoTime();
//
//        System.out.println("In " + ((t1-t0)/100000/10.0) +"ms");
    }

    @Test
    public void testFileStorage() throws Exception {
//        engine.loadJavaScriptModule("FileStorageTest", "FileStorageTest.js", ctx);
//
//        Transaction trx = new Transaction("I1001","120001","01", ctx);
//        trx.setClientId("XCH");
//        trx.setMessage(new DataObject("{}"));
//
//        engine.runJavaScriptWithTransaction("FileStorageTest", trx);
    }

    @Test
    public void testInstallNodeModule() throws Exception {
//        engine.installNodeJSModule("md5-node@1.0.1", ctx);
//
//        JavaScriptExecutor jsmain = new JavaScriptExecutor();
//        jsmain.loadScript("NodeTest", "JSBridgeTest.js", ctx);
//
//        JavaScriptExecutor js = new JavaScriptExecutor();
//        Transaction trx = new Transaction(null,null,null,ctx);
//        trx.setMessage(new DataObject("{\"text\":\"Hello\"}"));
//
//        long t0 = System.nanoTime();
//        js.runScriptWithinTransaction("NodeTest", trx);
//        long t1 = System.nanoTime();
//
//        System.out.println(trx.getMessage() + " in " + ((t1-t0)/100000/10.0) +"ms");
    }

    @Test
    public void testDoesNodeModuleExists() throws Exception {
//        System.out.println(engine.doesNodeJSModuleExist("md5-node@1.0.2"));
    }

    @Test
    public void testUninstallNodeModule() throws Exception {
//        engine.uninstallNodeJSModule("md5-node", ctx);
    }

    @Test
    public void testRunScriptFile() throws Exception {
//        System.out.println(engine.runJavaScriptFile("SimpleIntegration.js", false, true, ctx));

//        System.out.println(engine.runJavaScriptFile("SimpleNode.js", false, true, ctx));
//        Thread.sleep(6000);
//        System.out.println(engine.runJavaScriptFile("SimpleNode.js", false, true, ctx));
    }

    @Test
    public void testRunScriptFileAsync() throws Exception {
//        System.out.println(engine.runJavaScriptFile("SimpleNodeAsync.js", true, ctx));
    }

    @Test
    public void testRunScript() throws Exception {
//        System.out.println(engine.runJavaScript("(function(){return \"haha\"})()", null));
    }

}
