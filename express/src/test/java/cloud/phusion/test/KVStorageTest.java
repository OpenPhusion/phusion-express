package cloud.phusion.test;

import cloud.phusion.Engine;
import cloud.phusion.EngineFactory;
import cloud.phusion.storage.KVStorage;
import org.junit.*;

import java.util.Properties;

public class KVStorageTest {
    private static KVStorage storage;

    @BeforeClass
    public static void setUp() throws Exception {
        Properties props = new Properties();

//        props.setProperty(EngineFactory.Redis_Host, "192.168.1.1");
//        props.setProperty(EngineFactory.Redis_Port, "1111");
//        props.setProperty(EngineFactory.Redis_Database, "0");
//        props.setProperty(EngineFactory.Redis_Auth, "123456");

        Engine engine = EngineFactory.createEngine(props);

        storage = engine.getKVStorageForIntegration("Simple");
    }

    @Test
    public void testKVSave() throws Exception {
//        storage.put("vehicle", "A12345");
//
//        assertTrue(storage.doesExist("vehicle"));
//
//        String value = (String)storage.get("vehicle");
//        assertEquals(value, "A12345");
//
//        storage.put("vehicle", "B23456");
//
//        value = (String)storage.get("vehicle");
//        assertEquals(value, "B23456");
    }

    @Test
    public void testKVRemove() throws Exception {
//        storage.remove("vehicle");
//
//        assertFalse(storage.doesExist("vehicle"));
//
//        String value = (String)storage.get("vehicle");
//        assertNull(value);
    }

    @Test
    public void testKVTimeout() throws Exception {
//        String value;
//
//        storage.put("vehicle", "A12345");
//        storage.put("vehicle", "B23456", 900l);
//
//        assertTrue(storage.doesExist("vehicle"));
//
//        value = (String)storage.get("vehicle");
//        assertEquals(value, "B23456");
//
//        Thread.sleep(1000l);
//
//        assertFalse(storage.doesExist("vehicle"));
//
//        value = (String)storage.get("vehicle");
//        assertNull(value);
    }

    @Test
    public void testLock() throws Exception {
//        if (storage.lock("A")) {
//            assertFalse(storage.lock("A"));
//            storage.unlock("A");
//        }
    }

    @AfterClass
    public static void tearDown() {
    }
}
