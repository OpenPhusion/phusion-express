package cloud.phusion.test;

import cloud.phusion.*;
import org.junit.*;

import java.util.Properties;

public class ApplicationTest {
    private static Engine engine = null;
    private static Context ctx = null;

    @BeforeClass
    public static void setUp() throws Exception {
        Properties props = new Properties();
//        props.setProperty(EngineFactory.HttpServer_Enabled, "true");
        engine = EngineFactory.createEngine(props);
        ctx = EngineFactory.createContext(engine);
        engine.start(ctx);
    }

    @Test
    public void testApplicationManagement() throws Exception {
//        String className = "cloud.phusion.test.util.SimpleApp";
//        String appId = "AppSimple";
//        String config = "{}";
//
//        assertEquals( engine.getApplicationStatus(appId), ExecStatus.None );
//
//        engine.registerApplication( appId, className, new DataObject(config), null );
//        assertEquals( engine.getApplicationStatus(appId), ExecStatus.Stopped );
//        Application app = engine.getApplication(appId);
//
//        app.start(ctx);
//        assertEquals( engine.getApplicationStatus(appId), ExecStatus.Running );
//
//        try {
//            engine.removeApplication(appId, null);
//            fail();
//        } catch (Exception ex) {
//            assertTrue( ex instanceof PhusionException );
//        }
//
//        app.stop(ctx);
//        assertEquals( engine.getApplicationStatus(appId), ExecStatus.Stopped );
//
//        engine.removeApplication(appId, null);
//        assertEquals( engine.getApplicationStatus(appId), ExecStatus.None );
    }

    @Test
    public void testConnectionManagement() throws Exception {
//        String className = "cloud.phusion.test.util.SimpleApp";
//        String appId = "AppSimple";
//        String connId = "LuyaoConn";
//        String config = "{}";
//
//        engine.registerApplication( appId, className, new DataObject(config), null );
//        Application app = engine.getApplication(appId);
//        app.start(ctx);
//
//        assertEquals( app.getConnectionStatus(appId), ConnectionStatus.None );
//
//        app.createConnection(connId, null, ctx);
//        assertEquals( app.getConnectionStatus(connId), ConnectionStatus.Unconnected );
//
//        app.connect(connId, ctx);
//        assertEquals( app.getConnectionStatus(connId), ConnectionStatus.Connected );
//
//        try {
//            app.removeConnection(connId, ctx);
//            fail();
//        } catch (Exception ex) {
//            assertTrue( ex instanceof PhusionException );
//        }
//
//        app.disconnect(connId, ctx);
//        assertEquals( app.getConnectionStatus(connId), ConnectionStatus.Unconnected );
//
//        app.removeConnection(connId, ctx);
//        assertEquals( app.getConnectionStatus(appId), ConnectionStatus.None );
//
//        app.stop(ctx);
//        engine.removeApplication(appId, null);
    }

    @AfterClass
    public static void tearDown() {
    }

}
