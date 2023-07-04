package cloud.phusion.express;

import cloud.phusion.Context;
import cloud.phusion.DataObject;
import cloud.phusion.Engine;
import cloud.phusion.EngineFactory;
import cloud.phusion.express.controller.*;
import cloud.phusion.express.integration.IntegrationImpl;
import cloud.phusion.express.service.*;
import cloud.phusion.express.util.ErrorResponse;
import cloud.phusion.express.util.ServiceLogger;
import cloud.phusion.protocol.http.HttpMethod;
import cloud.phusion.protocol.http.HttpRequest;
import cloud.phusion.protocol.http.HttpResponse;
import cloud.phusion.protocol.http.HttpServer;
import com.alibaba.fastjson2.JSONObject;

import java.io.InputStream;
import java.util.Properties;

public class ExpressService implements HttpServer {
    public static final String STORAGE_ID = "phusion";

    public static final String JAR_PATH = "jar";
    public static final String CODE_PATH = "code";
    public static final String PROP_FILE = "application.properties";

    private static final String _position = ExpressService.class.getName();
    private static String _corsOrigin = null;

    public static void main(String[] args) throws Exception {
        Engine engine;

        // Init and start the engine

        Properties props = _loadProperties(args);

        engine = EngineFactory.createEngine(props);
        Context ctx = EngineFactory.createContext(engine);

        if (args.length>0 && args[0].equals("preparePhusionTables")) {
            // Prepare DB tables only

            ExpressService.prepareDBTables(props, ctx);
            System.out.println();
            System.out.println("Phusion tables prepared.");

            // Exit the process
            System.exit(0);
        }

        engine.start(ctx);

        _initServices(props, ctx);

        // Start applications and integrations

        ApplicationService.startAllApplications(ctx);
        IntegrationService.startAllIntegrations(ctx);

        // Start the ExpressService web service

        HttpServer server = new ExpressService();
        engine.registerHttpServer("/service/v1/{_category}", server, ctx);
        engine.registerHttpServer("/service/v1/{_category}/{_item}", server, ctx);
    }

    @Override
    public void handle(HttpRequest req, HttpResponse resp, Context ctx) throws Exception {
        if (req.getMethod().equals(HttpMethod.OPTIONS)) {
            // For CORS preflight request
            resp.setStatusCode(200);
            resp.setBody(new DataObject(""));
            _setCORSHeaders(resp);
            return;
        }

        String category = req.getParameter("_category");

        try {
            // Fetch user credentials

            String token = req.getHeader(UserService.HEADER_TOKEN);
            JSONObject user = null;

            if (token!=null && token.length()>0) {
                user = UserService.getCurrentUser(token, ctx);
                if (user != null) {
                    String userInfo = user.getString("id") + " (" + user.getString("name") + ")";
                    ctx.setContextInfo("user", userInfo);
                    ServiceLogger.info(_position, "Received user request", "traceId="+ctx.getId()+", user="+userInfo);
                }
            }

            // Process the request

            switch (category) {
                case "application":
                    ApplicationController.dispatch(user, req, resp, ctx);
                    break;
                case "client":
                    ClientController.dispatch(user, req, resp, ctx);
                    break;
                case "connection":
                    ConnectionController.dispatch(user, req, resp, ctx);
                    break;
                case "cluster":
                    ClusterController.dispatch(user, req, resp, ctx);
                    break;
                case "module":
                    ModuleController.dispatch(user, req, resp, ctx);
                    break;
                case "transaction":
                    TransactionController.dispatch(user, req, resp, ctx);
                    break;
                case "integration":
                    IntegrationController.dispatch(user, req, resp, ctx);
                    break;
                case "user":
                    UserController.dispatch(user, req, resp, ctx);
                    break;
                default:
                    resp.setBody(ErrorResponse.compose("BAD_REQ_URL"));
            }
        } catch (Exception ex) {
            ServiceLogger.error(_position, "Failed to process request", "traceId="+ctx.getId(), ex);

            StringBuilder data = new StringBuilder();
            data.append("{\"exception\":\"").append(ex.getClass().getName()).append(": ")
                    .append(ex.getMessage()).append("\", \"trace\":\"").append(ctx.getId()).append("\"}");

            resp.setBody(ErrorResponse.compose("SYS_ERR", data.toString()));
        }

        if (! resp.getHeaders().containsKey("Content-Type"))
            resp.setHeader("Content-Type", "application/json; charset=UTF-8");

        _setCORSHeaders(resp);
    }

    private static void _setCORSHeaders(HttpResponse resp) {
        if (_corsOrigin == null) return;

        resp.setHeader("Access-Control-Allow-Origin", _corsOrigin);
        resp.setHeader("Access-Control-Allow-Methods", "*");
        resp.setHeader("Access-Control-Allow-Headers", "*");
        resp.setHeader("Access-Control-Max-Age", "3600");
    }

    private static Properties _loadProperties(String[] args) throws Exception {
        Properties props = new Properties();

        // Load from application.properties

        try (InputStream in = ExpressService.class.getClassLoader().getResourceAsStream(PROP_FILE)) {
            props.load(in);
        }

        // Load from environment

        String dcId = System.getenv("PHUSION_DCID");
        if (dcId!=null && dcId.length()>0) props.setProperty(EngineFactory.Cluster_DataCenter, dcId);

        String wId = System.getenv("PHUSION_WID");
        if (wId!=null && wId.length()>0) props.setProperty(EngineFactory.Cluster_Worker, wId);

        String svcaddr = System.getenv("PHUSION_SVCADDR");
        if (svcaddr!=null && svcaddr.length()>0) props.setProperty(ClusterService.SVC_ADDR, svcaddr);

        // Load from command line

        for (String arg : args) {
            int pos = arg.indexOf('=');
            if (pos <= 0) continue;
            props.setProperty(arg.substring(0,pos), arg.substring(pos+1));
        }

        String path = props.getProperty(EngineFactory.FileStorage_PrivateRootPath);
        String pathJar = String.join("/", path, STORAGE_ID, JAR_PATH, "");
        String pathJs = String.join("/", path, STORAGE_ID, CODE_PATH, "");
        props.setProperty(EngineFactory.Module_JavaFileRootPath, pathJar);
        props.setProperty(EngineFactory.Module_JavaScriptFileRootPath, pathJs);

        boolean trxLogEnabled = Boolean.parseBoolean(props.getProperty(TransactionService.TRXLog_Enabled, "false"));
        if (trxLogEnabled) {
            props.setProperty(EngineFactory.TRXLog_Target, STORAGE_ID);
            props.setProperty(EngineFactory.TRXLog_EncodeMsg, "true");
        }

        // Log content of properties, desensitizing the secrets

        Properties clonedProps = (Properties) props.clone();
        clonedProps.setProperty(UserService.SECRET_KEY, "******");
        clonedProps.setProperty(EngineFactory.JDBC_Password, "******");
        clonedProps.setProperty(EngineFactory.Redis_Auth, "******");

        // Get CORS Origin
        String origin = props.getProperty("http.server.corsOrigin");
        if (origin!=null && origin.length()>0) _corsOrigin = origin;

        ServiceLogger.info(_position, "Engine and Service configuration: "+clonedProps.toString());

        return props;
    }

    private static void _initServices(Properties props, Context ctx) throws Exception {
        boolean prepareTables = Boolean.parseBoolean( props.getProperty("db.storage.preparePhusionTables", "true") );
        boolean trxLogEnabled = Boolean.parseBoolean(props.getProperty(TransactionService.TRXLog_Enabled, "false"));

        if (prepareTables && trxLogEnabled) {
            IntegrationImpl.prepareDBTables(ExpressService.STORAGE_ID, ctx);
        }

        ClusterService.init(
                props.getProperty(ClusterService.HEARTBEAT_INTERVAL, "10"),
                props.getProperty(ClusterService.SVC_ADDR, ""),
                prepareTables,
                ctx
        );

        ApplicationService.init(prepareTables, ctx);

        ClientService.init(prepareTables, ctx);

        ConnectionService.init(prepareTables, ctx);

        TransactionService.init(
                trxLogEnabled,
                props.getProperty(TransactionService.TRXLog_Stats_Interval, "3600"),
                prepareTables,
                ctx
        );

        IntegrationService.init(prepareTables, ctx);

        TagService.init(prepareTables, ctx);

        UserService.init(
                props.getProperty(UserService.SECRET_KEY),
                props.getProperty(UserService.SESSION_INTERVAL, "30"),
                prepareTables,
                ctx
        );

        ModuleService.init(props.getProperty(ModuleService.POM_LOCATION));
    }

    private static void prepareDBTables(Properties props, Context ctx) throws Exception {
        boolean trxLogEnabled = Boolean.parseBoolean(props.getProperty(TransactionService.TRXLog_Enabled, "false"));
        if (trxLogEnabled) IntegrationImpl.prepareDBTables(ExpressService.STORAGE_ID,ctx);

        ApplicationService.prepareDBTables(ctx);
        ClientService.prepareDBTables(ctx);
        ClusterService.prepareDBTables(ctx);
        ConnectionService.prepareDBTables(ctx);
        IntegrationService.prepareDBTables(ctx);
        TagService.prepareDBTables(ctx);
        TransactionService.prepareDBTables(ctx);
        UserService.prepareDBTables(ctx);
    }

}
