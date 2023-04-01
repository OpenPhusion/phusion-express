package cloud.phusion.test;

import cloud.phusion.Context;
import cloud.phusion.DataObject;
import cloud.phusion.Engine;
import cloud.phusion.EngineFactory;
import cloud.phusion.express.service.ApplicationService;
import cloud.phusion.express.service.IntegrationService;
import cloud.phusion.express.service.TagService;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Properties;

public class TagServiceTest {
    private static Context ctx;

    @BeforeClass
    public static void setUp() throws Exception {
        String base = TagServiceTest.class.getClassLoader().getResource("").getPath();

        Properties props = new Properties();

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

        Engine engine = EngineFactory.createEngine(props);
        ctx = EngineFactory.createContext(engine);
    }

    @Test
    public void testListTags() throws Exception {
//        DataObject result = TagService.listTags(TagService.TagType_App_Category, ctx);
//        if (result != null) System.out.println(result.getString());
    }

    @Test
    public void testGetTags() throws Exception {
//        DataObject result = TagService.getTags("1001", TagService.TagType_App_Category, ctx);
//        if (result != null) System.out.println(result.getString());
    }

    @Test
    public void testSaveTags() throws Exception {
//        String tags = "[\"aaa\",\"aaa\"]";
//        TagService.saveTags("1001", TagService.TagType_App_Category, new DataObject(tags), ctx);
    }

    @Test
    public void testRemoveTags() throws Exception {
//        TagService.removeTags("1001", ctx);
    }

    @AfterClass
    public static void tearDown() throws Exception {

    }

}
