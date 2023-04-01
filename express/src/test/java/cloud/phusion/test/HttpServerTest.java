package cloud.phusion.test;

import cloud.phusion.Context;
import cloud.phusion.DataObject;
import cloud.phusion.Engine;
import cloud.phusion.EngineFactory;
import cloud.phusion.protocol.http.*;
import org.apache.commons.io.FileUtils;
import org.junit.*;

import java.io.File;
import java.io.InputStream;
import java.util.Properties;
import java.util.Set;

import static org.junit.Assert.*;

public class HttpServerTest implements HttpServer {
    private static Engine engine;
    private static Context ctx;

    @BeforeClass
    public static void setUp() throws Exception {
//        Properties props = new Properties();
//        props.setProperty(EngineFactory.HttpServer_Enabled, "true");
//        props.setProperty(EngineFactory.HttpServer_Port, "9901");
//        engine = EngineFactory.createEngine(props);
//        ctx = EngineFactory.createContext(engine);
//
//        engine.start(ctx);
    }

    @Test
    public void testServer() throws Exception {
//        String path = "/order/{orderId}";
//        String pathConcret = "/order/123456?a=111&b=222";
//
//        engine.registerHttpServer(path, new HttpServerTest(), ctx);
//        assertTrue(engine.doesHttpServerExist(path));
//
//        HttpClient http = engine.createHttpClient();
//
//        // FORM
//        HttpResponse response = http
//                .post("http://localhost:9901"+pathConcret)
//                .header("Content-Type", "application/x-www-form-urlencoded; charset=UTF-8")
//                .body("c=333&d=444")
//                .context(ctx)
//                .send();
//
//        assertEquals(200, response.getStatusCode());
//        assertEquals("application/json;charset=UTF-8", response.getHeader("Content-Type"));
//        assertEquals("{\"status\":\"OK\"}", response.getBody().getString());
//
//        engine.unregisterHttpServer(path, ctx);
//        assertFalse(engine.doesHttpServerExist(path));
    }

    @Test
    public void testUploadFile() throws Exception {
        // Use upload.html for form-data
        // Or, use Postman to send a file, with query parameter "filename"

//        String path = "/upload";
//        String path = "/orders/{orderId}";
//
//        engine.registerHttpServer(path, new HttpServerTest(), ctx);
//        assertTrue(engine.doesHttpServerExist(path));
//
//        Thread.sleep(1000000);
    }

    @AfterClass
    public static void tearDown() throws Exception {
    }

    @Override
    public void handle(HttpRequest request, HttpResponse response, Context ctx) throws Exception {
        System.out.println("Method: "+request.getMethod());
        System.out.println("x-name: "+request.getHeader("x-name"));
        System.out.println("orderId: "+request.getParameter("orderId"));
        System.out.println("a: "+request.getParameter("a"));
        System.out.println("b: "+request.getParameter("b"));
        System.out.println("c: "+request.getParameter("c"));
        System.out.println("d: "+request.getParameter("d"));
        System.out.println("body: "+(request.getBody()==null ? null : request.getBody().getString()));

        if (request.hasFiles()) {
            String base = FileStorageTest.class.getClassLoader().getResource("").getPath();
            Set<String> files = request.getFileNames();

            for (String file : files) {
                try (InputStream in = request.getFileContent(file)) {
                    String filename = file.length()==0 ? request.getParameter("filename") : file;
                    FileUtils.copyInputStreamToFile(in, new File(base+filename));
                }
            }
        }

        response.setStatusCode(200);
        response.setHeader("Content-Type", "application/json; charset=UTF-8");
        response.setBody(new DataObject("{\"status\":\"OK\"}"));
    }
}
