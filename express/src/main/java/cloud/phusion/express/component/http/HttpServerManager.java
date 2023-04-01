package cloud.phusion.express.component.http;

import cloud.phusion.Context;
import cloud.phusion.DataObject;
import cloud.phusion.EngineFactory;
import cloud.phusion.PhusionException;
import cloud.phusion.express.util.TimeMarker;
import cloud.phusion.protocol.http.HttpRequest;
import cloud.phusion.protocol.http.HttpResponse;
import cloud.phusion.protocol.http.HttpServer;
import org.apache.catalina.connector.Connector;
import org.apache.catalina.startup.Tomcat;

import java.io.File;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Tomcat Configuration:
 * https://tomcat.apache.org/tomcat-9.0-doc/config/http.html
 */
public class HttpServerManager {
    private static final String _position = HttpServerManager.class.getName();

    private static Tomcat tomcat = null;
    private static boolean tomcatRunning = false;
    private static org.apache.catalina.Context tomcatContext = null;
    private static Map<String, HttpServer> servers = new ConcurrentHashMap<String, HttpServer>();
    private static Map<String, Context> serversContext = new ConcurrentHashMap<String, Context>();

    // Cache the parsed URL parts
    private static Map<String, String[]> paths = new ConcurrentHashMap<String, String[]>();

    public static void init(int port, Properties params) throws Exception {
        tomcat = new Tomcat();
        tomcatRunning = false;

        Connector connector = tomcat.getConnector();
        connector.setPort(port);

        if (params.containsKey("maxPostSize")) connector.setProperty("maxPostSize", params.getProperty("maxPostSize"));
        if (params.containsKey("maxThreads")) connector.setProperty("maxThreads", params.getProperty("maxThreads"));
        if (params.containsKey("maxConnections")) connector.setProperty("maxConnections", params.getProperty("maxConnections"));
        if (params.containsKey("acceptCount")) connector.setProperty("acceptCount", params.getProperty("acceptCount"));

        tomcatContext = tomcat.addContext("", new File(".").getAbsolutePath()); // Base doc path: docBase
        tomcatContext.setCookies(true);

        if (params.containsKey("sessionTimeoutMinutes"))
            tomcatContext.setSessionTimeout( Integer.parseInt(params.getProperty("sessionTimeoutMinutes")) );
    }

    public static void startHttpRootServer() throws Exception {
        // One controller Servlet for all
        Tomcat.addServlet(tomcatContext, "PhusionController", new HttpPhusionController());
        tomcatContext.addServletMappingDecoded("/*", "PhusionController");

        tomcat.start();
        tomcatRunning = true;
    }

    public static void stopHttpRootServer() throws Exception {
        tomcat.stop();
        tomcatRunning = false;
    }

    public static void registerHttpServer(String path, HttpServer server, DataObject config, Context ctx) throws Exception {
        if (! tomcatRunning)
            throw new PhusionException("HTTP_WEB_NONE", "Failed to register HTTP server", ctx);
        if (servers.containsKey(path))
            throw new PhusionException("HTTP_EXIST", "Failed to register HTTP server", ctx);

        servers.put(path, server);
        paths.put(path, path.split("/"));
        serversContext.put(path, ctx);

        ctx.logInfo(_position, "Http server registered");
    }

    public static void unregisterHttpServer(String path, Context ctx) throws Exception {
        if (! servers.containsKey(path))
            throw new PhusionException("HTTP_NONE", "Failed to unregister HTTP server", ctx);

        servers.remove(path);
        paths.remove(path);
        serversContext.remove(path);

        ctx.logInfo(_position, "Http server unregistered");
    }

    public static boolean doesHttpServerExist(String path) {
        return servers.containsKey(path);
    }

    public static Map<String, String[]> getPaths() {
        return paths;
    }

    public static void runServer(String path, HttpRequest request, HttpResponse response) throws Exception {
        if (!servers.containsKey(path))
            throw new PhusionException("HTTP_NONE", "Failed to handle HTTP request", "path="+path);

        HttpServer server = servers.get(path);
        Context c = serversContext.get(path);
        Context ctx = EngineFactory.createContext(c.getEngine()); // Create a new context

        String strBody = request.getBody()==null ? "" : request.getBody().getString(500);
        ctx.logInfo(_position, "Handling HTTP request", String.format("method=%s, path=%s, headers=%s, parameters=%s, body=%s",
                request.getMethod(), request.getRelativeUrl(), request.getHeaders(), request.getParameters(), strBody));
        TimeMarker marker = new TimeMarker();

        try {
            server.handle(request, response, ctx);
        } catch (Exception ex) {
            throw new PhusionException("HTTP_OP", "Failed to handle HTTP request", ctx, ex);
        }

        double ms = marker.mark();
        ctx.logInfo(_position, "HTTP request handled with response", String.format("code=%d, headers=%s, body=%s, time=%.1fms",
                response.getStatusCode(), response.getHeaders(), response.getBody()==null?"":response.getBody().getString(500), ms));
    }

}
