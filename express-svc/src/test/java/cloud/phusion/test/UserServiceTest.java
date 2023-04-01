package cloud.phusion.test;

import cloud.phusion.Context;
import cloud.phusion.DataObject;
import cloud.phusion.Engine;
import cloud.phusion.EngineFactory;
import cloud.phusion.express.ExpressService;
import cloud.phusion.express.service.AuthorizationService;
import cloud.phusion.express.service.ClusterService;
import cloud.phusion.express.service.UserService;
import cloud.phusion.express.util.TimeMarker;
import cloud.phusion.storage.KVStorage;
import com.alibaba.fastjson2.JSONObject;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class UserServiceTest {
    private static Context ctx;

    @BeforeClass
    public static void setUp() throws Exception {
        String base = ModuleServiceTest.class.getClassLoader().getResource("").getPath();
        base = base.replace("target/test-classes/", "storage/");

        Properties props = new Properties();
        props.setProperty(EngineFactory.FileStorage_PrivateRootPath, base+"private");

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

        UserService.init("123456", "30", false, ctx);
    }

    @Test
    public void testListRoles() throws Exception {
//        DataObject result = UserService.listRolesFromDisk(ctx);
//        if (result != null) System.out.println(result.getString());
    }

    @Test
    public void testSaveRoles() throws Exception {
//        String str = "[" +
//                "{\"id\":\"admin\",\"title\":\"admin\",\"grant\":[\"* * *\"]}," +
//                "{\"id\":\"visitor\",\"title\":\"visitor\",\"grant\":[\"GET * *\"]}," +
//                "{\"id\":\"anonymous\",\"title\":\"anonymous\",\"grant\":[]}," +
//                "{\"id\":\"xxx\",\"title\":\"xxx\",\"grant\":[]}" +
//                "]";
//        UserService.saveRoles(new DataObject(str), ctx);
    }

    @Test
    public void testSaveUser() throws Exception {
//        String str = "{\"id\":\"bob\",\"password\":\"aaa\",\"name\":\"Bob\",\"phone\":\"1222022222\"," +
//                "\"email\":\"a@a.com\",\"desc\":\"It's Alice\",\"icon\":\"https://a.com/a.jpg\"," +
//                "\"permissions\":[{\"role\":\"visitor\",\"type\":\"client\",\"scope\":[]}]}";
//        UserService.saveUser("bob", new DataObject(str), true, true, true, ctx);
    }

    @Test
    public void testHasUser() throws Exception {
//        System.out.println( UserService.hasUser("aaa", ctx) );
    }

    @Test
    public void testQueryUsersById() throws Exception {
//        DataObject result = UserService.listUsersById("alice", null, ctx);
//        if (result != null) System.out.println(result.getString());
    }

    @Test
    public void testQueryUsers() throws Exception {
//        Map<String,Object> query = new HashMap<>();
//        DataObject result = UserService.queryUsers(query, "id", 0, 100, ctx);
//        if (result != null) System.out.println(result.getString());
    }

    @Test
    public void testRemoveUser() throws Exception {
//        UserService.removeUser("xuser", ctx);
    }

    @Test
    public void testRemoveRole() throws Exception {
//        UserService.removeRole("xxx", ctx);
    }

    @Test
    public void testPassword() throws Exception {
//        UserService.init("123456", "1", ctx);
//        String result = UserService.encodePassword("alice", "aaaa");
//        System.out.println(result);
    }

    @Test
    public void testLogin() throws Exception {
//        UserService.init("123456", "1", ctx);
//
//        String token = UserService.login("alice", "aaaa", ctx);
//        System.out.println();
//        System.out.println("Logged in: "+token);
//
//        JSONObject user = UserService.getCurrentUser(token, ctx);
//        System.out.println("Current user: "+(user==null ? "" : user.toJSONString()));
//
//        Thread.sleep(55000);
//
//        user = UserService.getCurrentUser(token, ctx);
//        System.out.println("Current user: "+(user==null ? "" : user.toJSONString()));
//
//        Thread.sleep(30000);
//
//        user = UserService.getCurrentUser(token, ctx);
//        System.out.println("Current user: "+(user==null ? "" : user.toJSONString()));
    }

    @AfterClass
    public static void tearDown() throws Exception {

    }

}
