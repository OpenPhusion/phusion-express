package cloud.phusion.test;

import cloud.phusion.Context;
import cloud.phusion.Engine;
import cloud.phusion.EngineFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

public class JavaModuleManagerTest {
    private Engine engine;
    private Context ctx;

    @Before
    public void setUp() throws Exception {
//        engine = EngineFactory.createEngine();
//        ctx = EngineFactory.createContext(engine);
    }

    @Test
    public void testLoadClass() throws Exception {
//        String base = JavaModuleManagerTest.class.getClassLoader().getResource("").getPath();
//
//        // The HelloImpl can be defined in the root class loader, or the current class loader (test2-0.1.jar).
//
//        assertFalse( engine.doesJavaModuleExist("M1") );
//        engine.loadJavaModule("M1", new String[]{base+"test1-0.1.jar"}, ctx);
//        assertTrue( engine.doesJavaModuleExist("M1") );
//
//        Runnable r = (Runnable) engine.createClassInstance("M1","com.mycompany.app.Hello", ctx);
//        r.run();
//
//        engine.unloadJavaModule("M1", ctx);
//        assertFalse( engine.doesJavaModuleExist("M1") );
    }

    @After
    public void tearDown() throws Exception {
    }

}
