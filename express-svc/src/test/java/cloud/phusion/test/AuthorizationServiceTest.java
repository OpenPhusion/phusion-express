package cloud.phusion.test;

import cloud.phusion.Context;
import cloud.phusion.DataObject;
import cloud.phusion.Engine;
import cloud.phusion.EngineFactory;
import cloud.phusion.express.service.AuthorizationService;
import cloud.phusion.protocol.http.HttpMethod;
import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONArray;
import com.alibaba.fastjson2.JSONObject;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.*;

public class AuthorizationServiceTest {
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

        Map<String,Object> roles = new HashMap<>();
        roles.put("admin", JSON.parseObject("{\"grant\":[\"* * *\"]}"));
        roles.put("coder", JSON.parseObject("{\"grant\":[\"* module code\"]}"));
        roles.put("supercoder", JSON.parseObject("{\"grant\":[\"* module code\",\"* module listallcode\"]}"));
        AuthorizationService.cacheRoles(roles);
    }

    private void _printResult(String position, String[] result) {
        System.out.println(
                (position==null ? "" : position+": ") +
                        (result==null ? "OK" : (result.length==1 ? result[0] : result[0]+" ("+result[1]+")"))
        );
    }

    @Test
    public void testUser() throws Exception {
//        String category = "user";
//        JSONObject user;
//        Map<String,Object> params;
//
//        user = JSON.parseObject("{\"id\":\"alice\"}");
//        params = new HashMap<>();
//        params.put("id", "alice");
//        _printResult("1. Save user", AuthorizationService.checkPrivilege(user, HttpMethod.PUT, category, null, params, ctx));
//
//        params.put("permissions", JSON.parseArray("[{\"role\":\"\",\"scope\":\"123\",\"type\":\"client\"}]"));
//        _printResult("2. Save user (SHOULD FAIL)", AuthorizationService.checkPrivilege(user, HttpMethod.PUT, category, null, params, ctx));
//
//        user = JSON.parseObject("{\"id\":\"bob\",\"clientId\":\"luyao\",\"permissions\":[{\"role\":\"admin\"}]}");
//        params = new HashMap<>();
//        params.put("id", "alice");
//        params.put("clientId", "luyao");
//        _printResult("3. Save user", AuthorizationService.checkPrivilege(user, HttpMethod.PUT, category, null, params, ctx));
//
//        user = JSON.parseObject("{\"id\":\"alice\",\"permissions\":[{\"role\":\"admin\"}]}");
//        _printResult("4. Save role", AuthorizationService.checkPrivilege(user, HttpMethod.PUT, category, "role", null, ctx));
//
//        user = JSON.parseObject("{\"id\":\"alice\"}");
//        _printResult("5. List roles", AuthorizationService.checkPrivilege(user, HttpMethod.GET, category, "role", null, ctx));
//
//        user = JSON.parseObject("{\"id\":\"alice\",\"clientId\":\"luyao\",\"permissions\":[{\"role\":\"admin\"}]}");
//        params = new HashMap<>();
//        params.put("clientId", "luyao");
//        _printResult("6. List/count users", AuthorizationService.checkPrivilege(user, HttpMethod.GET, category, null, params, ctx));
//
//        user = JSON.parseObject("{\"id\":\"alice\",\"clientId\":\"luyaoxx\",\"permissions\":[{\"role\":\"admin\"}]}");
//        params = new HashMap<>();
//        params.put("id", "alice");
//        params.put("clientId", "luyao");
//        _printResult("7. Get user", AuthorizationService.checkPrivilege(user, HttpMethod.GET, category, "id", params, ctx));
//
//        user = JSON.parseObject("{\"id\":\"alice\",\"permissions\":[{\"role\":\"admin\"}]}");
//        _printResult("8. Remove role", AuthorizationService.checkPrivilege(user, HttpMethod.DELETE, category, "role", null, ctx));
//
//        user = JSON.parseObject("{\"id\":\"alice\",\"clientId\":\"luyao\",\"permissions\":[{\"role\":\"admin\"}]}");
//        params = new HashMap<>();
//        params.put("id", "alice");
//        _printResult("9. Remove user", AuthorizationService.checkPrivilege(user, HttpMethod.DELETE, category, "id", params, ctx));
//
//        user = JSON.parseObject("{\"id\":\"alice\",\"clientId\":\"luyao\",\"permissions\":[{\"role\":\"admin\"}]}");
//        params = new HashMap<>();
//        params.put("id", "alice");
//        _printResult("10. Logout user", AuthorizationService.checkPrivilege(user, HttpMethod.DELETE, category, "login", params, ctx));
    }

    @Test
    public void testTransaction() throws Exception {
//        String category = "transaction";
//        JSONObject user;
//        Map<String,Object> params;
//
//        user = JSON.parseObject("{\"id\":\"alice\",\"clientId\":\"luyao\",\"permissions\":[{\"role\":\"admin\"}]}");
//        params = new HashMap<>();
//        params.put("clientId", "luyao");
//        _printResult("1. List transactions", AuthorizationService.checkPrivilege(user, HttpMethod.GET, category, null, params, ctx));
//
//        user = JSON.parseObject("{\"id\":\"alice\",\"clientId\":\"luyao\",\"permissions\":[{\"role\":\"admin\"}]}");
//        params = new HashMap<>();
//        params.put("integrationId", "IT1001");
//        _printResult("2. List transactions", AuthorizationService.checkPrivilege(user, HttpMethod.GET, category, null, params, ctx));
//
//        user = JSON.parseObject("{\"id\":\"alice\",\"permissions\":[{\"role\":\"admin\"}]}");
//        params = new HashMap<>();
//        params.put("applicationId", "eco");
//        _printResult("3. List transactions", AuthorizationService.checkPrivilege(user, HttpMethod.GET, category, null, params, ctx));
//
//        user = JSON.parseObject("{\"id\":\"alice\",\"clientId\":\"luyao\",\"permissions\":[{\"role\":\"admin\"}]}");
//        params = new HashMap<>();
//        params.put("integrationId", "IT1001");
//        _printResult("4. Get transaction", AuthorizationService.checkPrivilege(user, HttpMethod.GET, category, "id", params, ctx));
//
//        user = JSON.parseObject("{\"id\":\"alice\",\"clientId\":\"luyao\",\"permissions\":[{\"role\":\"admin\"}]}");
//        params = new HashMap<>();
//        params.put("integrationId", "IT1001");
//        _printResult("5. Get transaction step stats", AuthorizationService.checkPrivilege(user, HttpMethod.GET, category, "step", params, ctx));
    }

    @Test
    public void testConnection() throws Exception {
//        String category = "connection";
//        JSONObject user;
//        Map<String,Object> params;
//
//        user = JSON.parseObject("{\"id\":\"alice\",\"clientId\":\"luyao\",\"permissions\":[{\"role\":\"admin\"}]}");
//        params = new HashMap<>();
//        params.put("clientId", "luyao");
//        _printResult("1. Save connection", AuthorizationService.checkPrivilege(user, HttpMethod.PUT, category, null, params, ctx));
//
//        user = JSON.parseObject("{\"id\":\"alice\",\"clientId\":\"luyao\",\"permissions\":[{\"role\":\"admin\"}]}");
//        params = new HashMap<>();
//        params.put("clientId", "luyao");
//        _printResult("2. Get connection", AuthorizationService.checkPrivilege(user, HttpMethod.GET, category, "id", params, ctx));
//
//        user = JSON.parseObject("{\"id\":\"alice\",\"clientIdxx\":\"luyaoxx\",\"permissions\":[{\"role\":\"admin\"}]}");
//        params = new HashMap<>();
//        params.put("clientId", "luyao");
//        _printResult("3. List connection", AuthorizationService.checkPrivilege(user, HttpMethod.GET, category, null, params, ctx));
//
//        user = JSON.parseObject("{\"id\":\"alice\",\"clientId\":\"luyao\",\"permissions\":[{\"role\":\"admin\"}]}");
//        params = new HashMap<>();
//        params.put("id", "eco-10001");
//        _printResult("4. Operate connection", AuthorizationService.checkPrivilege(user, HttpMethod.POST, category, "operation", params, ctx));
//
//        user = JSON.parseObject("{\"id\":\"alice\",\"clientId\":\"luyao\",\"permissions\":[{\"role\":\"admin\"}]}");
//        params = new HashMap<>();
//        params.put("id", "eco-10001");
//        _printResult("5. Remove connection", AuthorizationService.checkPrivilege(user, HttpMethod.DELETE, category, "id", params, ctx));
    }

    @Test
    public void testIntegration() throws Exception {
        String category = "integration";
        JSONObject user;
        Map<String,Object> params;

        Set<Object> itIds = new HashSet<>();
        itIds.add("IT1001");
        itIds.add("IT1003");

        user = JSON.parseObject("{\"id\":\"alice\",\"clientId\":\"luyao\",\"permissions\":[{\"role\":\"admin\"}]}");
        params = new HashMap<>();
        params.put("clientId", "luyaoxx");
        params.put("id", "IT1001");
        _printResult("1. Save integration", AuthorizationService.checkPrivilege(user, HttpMethod.PUT, category, null, params, ctx));

        user = JSON.parseObject("{\"id\":\"alice\",\"clientId\":\"luyao\",\"permissions\":[{\"role\":\"admin\"}]}");
        params = new HashMap<>();
        params.put("clientId", "luyao");
        params.put("integrationId", "IT1001");
        _printResult("2. Save table", AuthorizationService.checkPrivilege(user, HttpMethod.PUT, category, "table", params, ctx));

        user = JSON.parseObject("{\"id\":\"alice\",\"clientId\":\"luyao\",\"permissions\":[{\"role\":\"admin\"}]}");
        params = new HashMap<>();
        params.put("id", "IT1001");
        _printResult("3. Test integration", AuthorizationService.checkPrivilege(user, HttpMethod.POST, category, "test", params, ctx));

        user = JSON.parseObject("{\"id\":\"alice\",\"clientId\":\"luyao\",\"permissions\":[{\"role\":\"admin\"}]}");
        params = new HashMap<>();
        params.put("id", "IT1001");
        _printResult("4. Operation integration", AuthorizationService.checkPrivilege(user, HttpMethod.POST, category, "operation", params, ctx));

        user = JSON.parseObject("{\"id\":\"alice\",\"clientId\":\"luyao\",\"permissions\":[{\"role\":\"admin\"}]}");
        params = new HashMap<>();
        params.put("id", "IT1001");
        _printResult("5. Remove integration", AuthorizationService.checkPrivilege(user, HttpMethod.DELETE, category, "id", params, ctx));

        user = JSON.parseObject("{\"id\":\"alice\",\"clientId\":\"luyao\",\"permissions\":[{\"role\":\"admin\"}]}");
        params = new HashMap<>();
        params.put("id", "IT1001");
        _printResult("6. Remove table", AuthorizationService.checkPrivilege(user, HttpMethod.DELETE, category, "table", params, ctx));

        user = JSON.parseObject("{\"id\":\"alice\",\"clientId\":\"luyao\",\"permissions\":[{\"role\":\"admin\"}]}");
        params = new HashMap<>();
        params.put("clientId", "luyao");
        _printResult("7. List integrations", AuthorizationService.checkPrivilege(user, HttpMethod.GET, category, null, params, ctx));

        user = JSON.parseObject("{\"id\":\"alice\",\"clientId\":\"luyao\",\"permissions\":[{\"role\":\"admin\",\"scope\":[\"luyao\"],\"type\":\"client\"}]}");
        params = new HashMap<>();
        params.put("integrationIds", itIds);
        _printResult("8. List integrations", AuthorizationService.checkPrivilege(user, HttpMethod.GET, category, null, params, ctx));

        user = JSON.parseObject("{\"id\":\"alice\",\"clientId\":\"luyao\",\"permissions\":[{\"role\":\"admin\",\"scope\":[\"luyao\"],\"type\":\"client\"}]}");
        params = new HashMap<>();
        params.put("clientId", "luyaoss");
        params.put("integrationId", "IT1001");
        _printResult("9. Get table", AuthorizationService.checkPrivilege(user, HttpMethod.GET, category, "table", params, ctx));

        user = JSON.parseObject("{\"id\":\"alice\",\"clientId\":\"luyao\",\"permissions\":[{\"role\":\"admin\"}]}");
        params = new HashMap<>();
        params.put("clientId", "luyao");
        params.put("integrationId", "IT1001");
        _printResult("10. Get integration", AuthorizationService.checkPrivilege(user, HttpMethod.GET, category, "id", params, ctx));

        user = JSON.parseObject("{\"id\":\"alice\",\"clientId\":\"luyao\",\"permissions\":[{\"role\":\"admin\"}]}");
        params = new HashMap<>();
        params.put("id", "IT1001");
        _printResult("11. list categories", AuthorizationService.checkPrivilege(user, HttpMethod.GET, category, "category", params, ctx));

        user = JSON.parseObject("{\"id\":\"alice\",\"clientId\":\"luyao\",\"permissions\":[{\"role\":\"admin\"}]}");
        params = new HashMap<>();
        params.put("id", "IT1001");
        _printResult("12. list templates", AuthorizationService.checkPrivilege(user, HttpMethod.GET, category, "template", params, ctx));
    }

    @Test
    public void testModule() throws Exception {
//        String category = "module";
//        JSONObject user;
//        Map<String,Object> params;
//
//        user = JSON.parseObject("{\"id\":\"alice\",\"permissions\":[{\"role\":\"admin\"}]}");
//        _printResult("1. Save java module", AuthorizationService.checkPrivilege(user, HttpMethod.PUT, category, "java", null, ctx));
//
//        user = JSON.parseObject("{\"id\":\"alice\",\"permissions\":[{\"role\":\"admin\"}]}");
//        _printResult("2. Save jar", AuthorizationService.checkPrivilege(user, HttpMethod.POST, category, "java", null, ctx));
//
//        user = JSON.parseObject("{\"id\":\"alice\",\"permissions\":[{\"role\":\"admin\"}]}");
//        _printResult("3. Install nodejs", AuthorizationService.checkPrivilege(user, HttpMethod.POST, category, "nodejs", null, ctx));
//
//        user = JSON.parseObject("{\"id\":\"alice\",\"permissions\":[{\"role\":\"admin\"}]}");
//        params = new HashMap<>();
//        params.put("owner", "alice");
//        _printResult("4. Save/run code", AuthorizationService.checkPrivilege(user, HttpMethod.POST, category, "code", params, ctx));
//
//        user = JSON.parseObject("{\"id\":\"alice\",\"clientId\":\"luyao\",\"permissions\":[{\"role\":\"admin\"}]}");
//        params = new HashMap<>();
//        params.put("owner", "integration");
//        params.put("filename", "IT1001.node.js");
//        _printResult("5. Save/run code", AuthorizationService.checkPrivilege(user, HttpMethod.POST, category, "code", params, ctx));
//
//        user = JSON.parseObject("{\"id\":\"alice\",\"permissions\":[{\"role\":\"admin\"}]}");
//        _printResult("6. Remove jar", AuthorizationService.checkPrivilege(user, HttpMethod.DELETE, category, "java", null, ctx));
//
//        user = JSON.parseObject("{\"id\":\"alice\",\"permissions\":[{\"role\":\"admin\"}]}");
//        _printResult("7. Uninstall nodejs", AuthorizationService.checkPrivilege(user, HttpMethod.DELETE, category, "nodejs", null, ctx));
//
//        user = JSON.parseObject("{\"id\":\"alice\",\"permissions\":[{\"role\":\"admin\"}]}");
//        params = new HashMap<>();
//        params.put("owner", "alice");
//        _printResult("8. Remove code", AuthorizationService.checkPrivilege(user, HttpMethod.DELETE, category, "code", params, ctx));
//
//        user = JSON.parseObject("{\"id\":\"alice\",\"clientId\":\"luyao\",\"permissions\":[{\"role\":\"admin\"}]}");
//        params = new HashMap<>();
//        params.put("owner", "integration");
//        params.put("filename", "IT1001.node.js");
//        _printResult("9. Remove code", AuthorizationService.checkPrivilege(user, HttpMethod.DELETE, category, "code", params, ctx));
//
//        user = JSON.parseObject("{\"id\":\"alice\",\"permissions\":[{\"role\":\"admin\"}]}");
//        _printResult("10. Get java module", AuthorizationService.checkPrivilege(user, HttpMethod.GET, category, "java", null, ctx));
//
//        user = JSON.parseObject("{\"id\":\"alice\",\"permissions\":[{\"role\":\"admin\"}]}");
//        _printResult("11. List nodejs modules", AuthorizationService.checkPrivilege(user, HttpMethod.GET, category, "nodejs", null, ctx));
//
//        user = JSON.parseObject("{\"id\":\"alice\",\"clientId\":\"luyao\",\"permissions\":[{\"role\":\"admin\"}]}");
//        _printResult("12. Get ID", AuthorizationService.checkPrivilege(user, HttpMethod.GET, category, "id", null, ctx));
//
//        user = JSON.parseObject("{\"id\":\"alice\",\"clientId\":\"luyao\",\"permissions\":[{\"role\":\"admin\"}]}");
//        _printResult("13. Encode", AuthorizationService.checkPrivilege(user, HttpMethod.GET, category, "encode", null, ctx));
//
//        user = JSON.parseObject("{\"id\":\"alice\",\"permissions\":[{\"role\":\"coder\"}]}");
//        params = new HashMap<>();
//        params.put("owner", "alice");
//        _printResult("14. Get code", AuthorizationService.checkPrivilege(user, HttpMethod.GET, category, "code", params, ctx));
//
//        user = JSON.parseObject("{\"id\":\"alice\",\"permissions\":[{\"role\":\"supercoder\"}]}");
//        params = new HashMap<>();
//        params.put("owner", "all");
//        _printResult("15. Get code", AuthorizationService.checkPrivilege(user, HttpMethod.GET, category, "code", params, ctx));
//
//        user = JSON.parseObject("{\"id\":\"alice\",\"clientId\":\"luyao\",\"permissions\":[{\"role\":\"admin\",\"scope\":[\"luyao\"],\"type\":\"client\"}]}");
//        params = new HashMap<>();
//        params.put("owner", "integration");
//        params.put("filename", "IT1001.node.js");
//        _printResult("16. Get code", AuthorizationService.checkPrivilege(user, HttpMethod.GET, category, "code", params, ctx));
    }

    @Test
    public void testApplication() throws Exception {
//        String category = "application";
//        JSONObject user;
//        Map<String,Object> params;
//
//        user = JSON.parseObject("{\"id\":\"alice\",\"permissions\":[{\"role\":\"admin\",\"typex\":\"application\",\"scopex\":[\"eco\"]}]}");
//        params = new HashMap<>();
//        params.put("id", "eco");
//        _printResult("1. Save application", AuthorizationService.checkPrivilege(user, HttpMethod.PUT, category, null, params, ctx));
//
//        user = JSON.parseObject("{\"id\":\"alice\",\"clientId\":\"luyao\",\"permissions\":[{\"role\":\"admin\"}]}");
//        _printResult("2. List application", AuthorizationService.checkPrivilege(user, HttpMethod.GET, category, null, null, ctx));
//
//        user = JSON.parseObject("{\"id\":\"alice\",\"permissions\":[{\"role\":\"admin\",\"type\":\"application\",\"scope\":[\"eco22\",\"eco\"]}]}");
//        params = new HashMap<>();
//        params.put("id", "eco");
//        _printResult("3. Remove application", AuthorizationService.checkPrivilege(user, HttpMethod.DELETE, category, "id", params, ctx));
    }

    @Test
    public void testClient() throws Exception {
//        String category = "client";
//        JSONObject user;
//        Map<String,Object> params;
//
//        Set<Object> cIds = new HashSet<>();
//        cIds.add("luyao");
//
//        user = JSON.parseObject("{\"id\":\"alice\",\"clientId\":\"luyao\",\"permissions\":[{\"role\":\"admin\"}]}");
//        params = new HashMap<>();
//        params.put("id", "luyao");
//        _printResult("1. Save client", AuthorizationService.checkPrivilege(user, HttpMethod.PUT, category, null, params, ctx));
//
//        user = JSON.parseObject("{\"id\":\"alice\",\"clientId\":\"luyao\",\"permissions\":[{\"role\":\"admin\"}]}");
//        params = new HashMap<>();
//        params.put("id", "luyao");
//        _printResult("2. Save table", AuthorizationService.checkPrivilege(user, HttpMethod.PUT, category, "table", params, ctx));
//
//        user = JSON.parseObject("{\"id\":\"alice\",\"clientIdxx\":\"luyao\",\"permissions\":[{\"role\":\"admin\"}]}");
//        params = new HashMap<>();
//        params.put("id", "luyao");
//        _printResult("3. Delete client", AuthorizationService.checkPrivilege(user, HttpMethod.DELETE, category, "id", params, ctx));
//
//        user = JSON.parseObject("{\"id\":\"alice\",\"clientId\":\"luyao\",\"permissions\":[{\"role\":\"admin\"}]}");
//        params = new HashMap<>();
//        params.put("id", "luyao");
//        _printResult("4. Delete table", AuthorizationService.checkPrivilege(user, HttpMethod.DELETE, category, "table", params, ctx));
//
//        user = JSON.parseObject("{\"id\":\"alice\",\"clientIdxx\":\"luyao\",\"permissions\":[{\"role\":\"admin\"}]}");
//        params = new HashMap<>();
//        params.put("id", "luyao");
//        _printResult("5. List categories", AuthorizationService.checkPrivilege(user, HttpMethod.GET, category, "category", params, ctx));
//
//        user = JSON.parseObject("{\"id\":\"alice\",\"clientId\":\"luyao\",\"permissions\":[{\"role\":\"admin\"}]}");
//        params = new HashMap<>();
//        params.put("clientIds", cIds);
//        _printResult("6. List clients", AuthorizationService.checkPrivilege(user, HttpMethod.GET, category, null, params, ctx));
//
//        user = JSON.parseObject("{\"id\":\"alice\",\"clientId\":\"luyao\",\"permissions\":[{\"role\":\"admin\"}]}");
//        params = new HashMap<>();
//        params.put("id", "luyao");
//        _printResult("7. Get client", AuthorizationService.checkPrivilege(user, HttpMethod.GET, category, "id", params, ctx));
    }

    @Test
    public void testPermissions() throws Exception {
//        String strPermissions = "[" +
//                "    {\"operations\": {\"GET user\": false, \"GET module *\": false}}," +
//                "    {" +
//                "        \"operations\": {\"GET transaction *\": false, \"PUT integration id\": false}," +
////                "        \"scopeType\": \"client\", \"scope\": [\"luyao\",\"luyao\"]" +
//                "        \"scopeType\": \"integration\", \"scope\": [\"IT1001\",\"IT1003\"]" +
//                "    }" +
//                "]";
//        String strUser = "{\"id\":\"alice\",\"clientId\":\"luyao\",\"permissions\":[{\"role\":\"admin\",\"type\":\"client\",\"scope\":[\"luyao\"]}]}";
//
//        DataObject result = AuthorizationService.checkPrivileges(JSON.parseObject(strUser), JSON.parseArray(strPermissions), ctx);
//        System.out.println( result==null ? null : result.getString() );
    }

    @AfterClass
    public static void tearDown() throws Exception {

    }

}
