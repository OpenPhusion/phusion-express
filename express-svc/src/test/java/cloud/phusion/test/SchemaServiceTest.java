package cloud.phusion.test;

import cloud.phusion.Context;
import cloud.phusion.DataObject;
import cloud.phusion.Engine;
import cloud.phusion.EngineFactory;
import cloud.phusion.express.service.SchemaService;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Properties;

public class SchemaServiceTest {
    private static Context ctx;

    @BeforeClass
    public static void setUp() throws Exception {
        String base = SchemaServiceTest.class.getClassLoader().getResource("").getPath();
        base = base.replace("target/test-classes/", "storage/");

        Properties props = new Properties();
        props.setProperty(EngineFactory.FileStorage_PrivateRootPath, base+"private");
        props.setProperty(EngineFactory.Module_JavaFileRootPath, base+"private/phusion/jar/");

        Engine engine = EngineFactory.createEngine(props);
        ctx = EngineFactory.createContext(engine);
    }

    @Test
    public void testHasSchema() throws Exception {
//        boolean result = SchemaService.hasSchema("eco.queryOrder.inputMessageSchema", ctx);
//        System.out.println(result ? "Yes" : "No");
    }

    @Test
    public void testFetchSchema() throws Exception {
//        DataObject schema = SchemaService.fetchSchema("eco.queryOrder.inputMessageSchema", ctx);
//        if (schema != null) System.out.println(schema.getJSONObject().toJSONString());
    }

    @Test
    public void testSaveSchema() throws Exception {
//        String id = "eco.queryOrder.inputMessageSchema";
//        String str = "{\"desc\":\"Demo schema\"}";
//
//        SchemaService.saveSchema(id, new DataObject(str), ctx);
//
//        DataObject schema = SchemaService.fetchSchema(id, ctx);
//        if (schema != null) System.out.println(schema.getJSONObject().toJSONString());
    }

    @Test
    public void testRemoveSchema() throws Exception {
//        String id = "eco.queryOrder.inputMessageSchema";
//        SchemaService.removeSchema(id, ctx);
    }

    @Test
    public void testRemoveSchemas() throws Exception {
//        String id = "eco.queryOrder";
//        SchemaService.removeSchemas(id, ctx);
    }

    @AfterClass
    public static void tearDown() throws Exception {

    }

}
