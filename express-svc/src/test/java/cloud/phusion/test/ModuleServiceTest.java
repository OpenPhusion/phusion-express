package cloud.phusion.test;

import cloud.phusion.Context;
import cloud.phusion.DataObject;
import cloud.phusion.Engine;
import cloud.phusion.EngineFactory;
import cloud.phusion.express.service.CodeRunner;
import cloud.phusion.express.service.ModuleService;
import cloud.phusion.express.service.SchemaService;
import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Properties;

import static org.junit.Assert.assertTrue;

public class ModuleServiceTest {
    private static Context ctx;

    @BeforeClass
    public static void setUp() throws Exception {
        String base = ModuleServiceTest.class.getClassLoader().getResource("").getPath();
        base = base.replace("target/test-classes/", "storage/");

        Properties props = new Properties();
        props.setProperty(EngineFactory.FileStorage_PrivateRootPath, base+"private");
        props.setProperty(EngineFactory.Module_JavaFileRootPath, base+"private/phusion/jar/");
        props.setProperty(EngineFactory.Module_JavaScriptFileRootPath, base+"private/phusion/code/");

        Engine engine = EngineFactory.createEngine(props);
        ctx = EngineFactory.createContext(engine);

        ModuleService.init(base+"../pom.xml");
    }

    @Test
    public void testLoadModule() throws Exception {
//        String module = "xcharge-v0.1.0";
//        String id = "xcharge";
//        ModuleService.loadJavaModule(module, ctx);
//        ctx.getEngine().registerApplication(id,  module, "com.xcharge.application.XCharge", null, ctx);
//
//        ctx.getEngine().removeApplication(id, ctx);
//        ctx.getEngine().unloadJavaModule(module, ctx);
    }

    @Test
    public void testSaveJavaModule() throws Exception {
//        String module = "xcharge-v0.1.0";
//        String str = "{\"module\":\""+module+"\",\"packages\":[{\"group\":\"a\",\"artifact\":\"b\",\"version\":\"c\"}]}";
//        ModuleService.saveJavaModule(module, new DataObject(str), ctx);
    }

    @Test
    public void testListJavaModules() throws Exception {
//        DataObject result = ModuleService.listJavaModules(ctx);
//        if (result != null) System.out.println(result.getString());
    }

    @Test
    public void testListNodeModules() throws Exception {
//        DataObject result = ModuleService.listNodeJSModules(ctx);
//        if (result != null) System.out.println(result.getString());
    }

    @Test
    public void testListCodes() throws Exception {
//        DataObject result = ModuleService.listCodes(ctx);
//        if (result != null) System.out.println(result.getString());
    }

    @Test
    public void testFetchCode() throws Exception {
//        DataObject result = ModuleService.fetchCode("sy", "a.txt", ctx);
//        if (result != null) System.out.println(result.getString());
    }

    @Test
    public void testSaveCode() throws Exception {
//        ModuleService.saveCode("sy", "a.js", new DataObject("hahahaha"), ctx);
    }

    @Test
    public void testRemoveCode() throws Exception {
//        ModuleService.removeCode("sy", "a.txt", ctx);
    }

    @Test
    public void testWrapCode() throws Exception {
//        String code = "console.log('hello');";
//
//        String code1 = CodeRunner.wrapCode(CodeRunner.CODE_TYPE_JS, code);
//        String code2 = CodeRunner.wrapCode(CodeRunner.CODE_TYPE_NODE, code);
//        String code3 = CodeRunner.wrapCode(CodeRunner.CODE_TYPE_DSL_I, code);
//        String code4 = CodeRunner.wrapCode(CodeRunner.CODE_TYPE_DSL_C, code);
//
//        System.out.println(CodeRunner.CODE_TYPE_JS + ":\n" + code1);
//        System.out.println();
//        System.out.println(CodeRunner.CODE_TYPE_NODE + ":\n" + code2);
//        System.out.println();
//        System.out.println(CodeRunner.CODE_TYPE_DSL_I + ":\n" + code3);
//        System.out.println();
//        System.out.println(CodeRunner.CODE_TYPE_DSL_C + ":\n" + code4);
//        System.out.println();
//
//        System.out.println(CodeRunner.unwrapCode(CodeRunner.CODE_TYPE_JS, code1));
//        System.out.println(CodeRunner.unwrapCode(CodeRunner.CODE_TYPE_NODE, code2));
//        System.out.println(CodeRunner.unwrapCode(CodeRunner.CODE_TYPE_DSL_I, code3));
//        System.out.println(CodeRunner.unwrapCode(CodeRunner.CODE_TYPE_DSL_C, code4));
    }

    @Test
    public void testCodeType() throws Exception {
//        System.out.println(CodeRunner.getTypeFromFilename("a/a.js"));
//        System.out.println(CodeRunner.getTypeFromFilename("a/a.node.js"));
//        System.out.println(CodeRunner.getTypeFromFilename("a/a.i.dsl.js"));
//        System.out.println(CodeRunner.getTypeFromFilename("a/a.c.dsl.json"));
//        System.out.println(CodeRunner.getTypeFromFilename("a/a.c.dsl.js"));
    }

    @Test
    public void testRunCode() throws Exception {
//        String code = "console.log('hello');";
//        String owner = "sy";
//        String ownerI = "integration";
//        String file = "a.js";
//
//        System.out.println("----JS----");
//        CodeRunner.runCode(CodeRunner.CODE_TYPE_JS, code, null, null, false, true, ctx);
//        CodeRunner.runCode(CodeRunner.CODE_TYPE_JS, null, owner, file, false, true, ctx);
//        CodeRunner.runCode(CodeRunner.CODE_TYPE_JS, code, owner, file, false, true, ctx);
//        CodeRunner.runCode(CodeRunner.CODE_TYPE_JS, null, null, file, false, true, ctx);
//
//        System.out.println("----NODE----");
//        CodeRunner.runCode(CodeRunner.CODE_TYPE_NODE, code, null, null, false, true, ctx);
//        CodeRunner.runCode(CodeRunner.CODE_TYPE_NODE, null, owner, file, false, true, ctx);
//        CodeRunner.runCode(CodeRunner.CODE_TYPE_NODE, code, ownerI, file, false, true, ctx);
//        CodeRunner.runCode(CodeRunner.CODE_TYPE_NODE, null, null, file, false, true, ctx);
//
//        System.out.println("----IDSL----");
//        CodeRunner.runCode(CodeRunner.CODE_TYPE_DSL_I, code, null, null, false, true, ctx);
//        CodeRunner.runCode(CodeRunner.CODE_TYPE_DSL_I, null, owner, file, false, true, ctx);
//        CodeRunner.runCode(CodeRunner.CODE_TYPE_DSL_I, code, owner, file, false, true, ctx);
//        CodeRunner.runCode(CodeRunner.CODE_TYPE_DSL_I, null, null, file, false, true, ctx);
//
//        System.out.println("----CDSL----");
//        CodeRunner.runCode(CodeRunner.CODE_TYPE_DSL_C, code, null, null, false, true, ctx);
//        CodeRunner.runCode(CodeRunner.CODE_TYPE_DSL_C, null, owner, file, false, true, ctx);
//        CodeRunner.runCode(CodeRunner.CODE_TYPE_DSL_C, code, owner, file, false, true, ctx);
//        CodeRunner.runCode(CodeRunner.CODE_TYPE_DSL_C, null, null, file, false, true, ctx);
    }

    @Test
    public void testRunCodeJS() throws Exception {
//        String str = "return 'hello';";
//        str = CodeRunner.wrapCode(CodeRunner.CODE_TYPE_JS, str);
//        System.out.println( CodeRunner.runJSCode(str, ctx) );
    }

    @Test
    public void testRunCodeNodeJS() throws Exception {
//        System.out.println( CodeRunner.runNodeJSCodeFile("sy", "d.i.dsl.js", false, true, ctx) );
    }

    @Test
    public void testRunCodeItegrationNodeJS() throws Exception {
//        String result = CodeRunner.runIntegrationCodeFile("sy", "b.js", false, true, ctx);
//        System.out.println();
//        System.out.println(result);
    }

    @Test
    public void testRunCodeCDSL() throws Exception {
        String code = "{" +
                "  \"expression\": \"(x==3.0 || x.equals(e)) && c.endsWith(d)\"," +
                "  \"vars\": {" +
                "    \"x\": {\"type\":\"Float\", \"fromMessage\":\"a.b\"}," +
                "    \"c\": {\"type\":\"String\", \"fromMessage\":\"c\"}," +
                "    \"d\": {\"type\":\"String\", \"fromConfig\":\"d\"}," +
                "    \"e\": {\"type\":\"Float\", \"value\":5.0}" +
                "  }," +
                "  \"msg\": {\"a\":[{\"b\":5.0}], \"c\":\"wwwVwww\"}," +
                "  \"config\": {\"d\":\"w\"}" +
                "}";

//        System.out.println( CodeRunner.runCDSLCode(code, ctx) );
    }

    @AfterClass
    public static void tearDown() throws Exception {

    }

}
