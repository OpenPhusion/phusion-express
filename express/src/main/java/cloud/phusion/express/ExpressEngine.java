package cloud.phusion.express;

import cloud.phusion.*;
import cloud.phusion.application.*;
import cloud.phusion.express.component.IDGenerator;
import cloud.phusion.express.component.JavaModuleManager;
import cloud.phusion.express.component.scripting.JavaScriptExecutor;
import cloud.phusion.express.component.storage.FileStorageImpl;
import cloud.phusion.express.component.storage.KVStorageImpl;
import cloud.phusion.express.component.storage.SQLDBStorageImpl;
import cloud.phusion.express.integration.ConditionEvaluator;
import cloud.phusion.express.integration.IntegrationImpl;
import cloud.phusion.integration.*;
import cloud.phusion.express.application.ApplicationManager;
import cloud.phusion.express.component.http.HttpClientImpl;
import cloud.phusion.express.component.http.HttpServerManager;
import cloud.phusion.express.component.scheduling.TaskManager;
import cloud.phusion.express.component.scheduling.TaskScheduler;
import cloud.phusion.express.integration.IntegrationManager;
import cloud.phusion.protocol.http.HttpClient;
import cloud.phusion.protocol.http.HttpServer;
import cloud.phusion.storage.DBStorage;
import cloud.phusion.storage.FileStorage;
import cloud.phusion.storage.KVStorage;
import com.alibaba.fastjson2.JSONObject;

import java.util.Date;
import java.util.Properties;

public class ExpressEngine implements Engine {
    private static final String _position = ExpressEngine.class.getName();

    private ExecStatus status;

    private ApplicationManager appManager = null;
    private IntegrationManager itManager = null;
    private TaskScheduler scheduler = null;
    private IDGenerator idGenerator = null;
    private JavaModuleManager moduleManager = null;
    private JavaScriptExecutor jsExecuter = null;

    private boolean startScheduler = false;
    private boolean startWebServer = false;

    private String engineId = null;
    private String dbType = null;

    private Properties config = null;

    public ExpressEngine(Properties props) throws Exception {
        super();

        config = (props==null) ? new Properties() : props;

        this.appManager = new ApplicationManager();
        this.itManager = new IntegrationManager();

        String dataCenter = config.getProperty(EngineFactory.Cluster_DataCenter);
        String worker = config.getProperty(EngineFactory.Cluster_Worker);
        if (dataCenter == null) dataCenter = "1";
        if (worker == null) worker = "1";
        this.engineId = dataCenter + "-" + worker;

        idGenerator = new IDGenerator(Long.parseLong(dataCenter), Long.parseLong(worker));
        moduleManager = new JavaModuleManager( config.getProperty(EngineFactory.Module_JavaFileRootPath) );
        jsExecuter = new JavaScriptExecutor( config.getProperty(EngineFactory.Module_JavaScriptFileRootPath) );

        KVStorageImpl.init(props);

        dbType = config.getProperty(EngineFactory.DB_Type);
        if (EngineFactory.DBType_JDBC.equals(dbType)) {
            SQLDBStorageImpl.init(props);
        }

        startScheduler = Boolean.parseBoolean( config.getProperty(EngineFactory.Scheduler_Enabled, "false") );
        startWebServer = Boolean.parseBoolean( config.getProperty(EngineFactory.HttpServer_Enabled, "false") );

        if (startScheduler) {
            this.scheduler = new TaskScheduler(props);
            TaskManager.init(props);
        }

        String port = config.getProperty(EngineFactory.HttpServer_Port, "9900");
        if (startWebServer) {
            Properties params = new Properties();
            if (config.containsKey(EngineFactory.HttpServer_MaxFileSize))
                params.setProperty("maxPostSize", config.getProperty(EngineFactory.HttpServer_MaxFileSize));

            HttpServerManager.init(Integer.parseInt(port), params);
        }

        status = ExecStatus.Stopped;
    }

    public ExpressEngine() throws Exception {
        this(null);
    }

    @Override
    public String getVersion() {
        return "phusion-engine-express v0.1";
    }

    @Override
    public String getId() {
        return engineId;
    }

    @Override
    public void start(Context ctx) throws Exception {
        if (status == ExecStatus.Running) return;
        if (ctx == null) ctx = EngineFactory.createContext(this);

        if (startScheduler) {
            scheduler.start();
        }
        if (startWebServer) {
            HttpServerManager.startHttpRootServer();
        }

        status = ExecStatus.Running;

        IntegrationImpl.initSystemSettings(
                config.getProperty(EngineFactory.TRXLog_Target),
                config.getProperty(EngineFactory.TRXLog_EncodeMsg, "true"),
                ctx
        );

        ctx.logInfo(_position, "Engine is running");
    }

    @Override
    public void stop(Context ctx) throws Exception {
        if (status == ExecStatus.Stopped) return;
        if (ctx == null) ctx = EngineFactory.createContext(this);

        if (startScheduler) {
            scheduler.stop();
        }

        if (startWebServer) {
            HttpServerManager.stopHttpRootServer();
        }

        status = ExecStatus.Stopped;

        ctx.logInfo(_position, "Engine is stopped");
    }

    @Override
    public ExecStatus getStatus() {
        return status;
    }

    @Override
    public void loadJavaModule(String moduleId, String[] paths, Context ctx) throws Exception {
        if (ctx == null) ctx = EngineFactory.createContext(this);
        ctx.setContextInfo("moduleId", moduleId);
        moduleManager.loadJar(moduleId, paths, ctx);
        ctx.removeContextInfo("moduleId");
    }

    @Override
    public void unloadJavaModule(String moduleId, Context ctx) throws Exception {
        if (ctx == null) ctx = EngineFactory.createContext(this);
        ctx.setContextInfo("moduleId", moduleId);
        moduleManager.unloadJar(moduleId, ctx);
        ctx.removeContextInfo("moduleId");
    }

    @Override
    public boolean doesJavaModuleExist(String moduleId) {
        return moduleManager.doesJarExist(moduleId);
    }

    @Override
    public Object createClassInstance(String moduleId, String className, Context ctx) throws Exception {
        if (ctx == null) ctx = EngineFactory.createContext(this);
        return moduleManager.createClassInstance(moduleId, className, ctx);
    }

    @Override
    public void loadJavaScriptModule(String moduleId, String path, Context ctx) throws Exception {
        if (ctx == null) ctx = EngineFactory.createContext(this);
        ctx.setContextInfo("moduleId", moduleId);
        jsExecuter.loadScript(moduleId, path, ctx);
        ctx.removeContextInfo("moduleId");
    }

    @Override
    public void installNodeJSModule(String nodejsModule, Context ctx) throws Exception {
        if (ctx == null) ctx = EngineFactory.createContext(this);
        ctx.setContextInfo("nodejsModule", nodejsModule);
        JavaScriptExecutor.installNodeModule(nodejsModule, ctx);
        ctx.removeContextInfo("nodejsModule");
    }

    @Override
    public void unloadJavaScriptModule(String moduleId, Context ctx) throws Exception {
        if (ctx == null) ctx = EngineFactory.createContext(this);
        ctx.setContextInfo("moduleId", moduleId);
        jsExecuter.unloadScript(moduleId, ctx);
        ctx.removeContextInfo("moduleId");
    }

    @Override
    public void uninstallNodeJSModule(String nodejsModule, Context ctx) throws Exception {
        if (ctx == null) ctx = EngineFactory.createContext(this);
        ctx.setContextInfo("nodejsModule", nodejsModule);
        JavaScriptExecutor.uninstallNodeModule(nodejsModule, ctx);
        ctx.removeContextInfo("nodejsModule");
    }

    @Override
    public DataObject listNodeJSModules(Context ctx) throws Exception {
        return JavaScriptExecutor.listNodeJSModules();
    }

    @Override
    public boolean doesNodeJSModuleExist(String nodejsModule) {
        return JavaScriptExecutor.doesNodeJSModuleExist(nodejsModule);
    }

    @Override
    public boolean doesJavaScriptModuleExist(String moduleId) {
        return jsExecuter.doesScriptExists(moduleId);
    }

    @Override
    public void runJavaScriptWithTransaction(String moduleId, Transaction trx) throws Exception {
        runJavaScriptWithTransaction(moduleId, trx, false);
    }

    @Override
    public void runJavaScriptWithTransaction(String moduleId, Transaction trx, boolean async) throws Exception {
        Context ctx = trx.getContext();
        if (ctx != null) ctx.setContextInfo("moduleId", moduleId);
        jsExecuter.runScriptWithinTransaction(moduleId, trx, async);
        if (ctx != null) ctx.removeContextInfo("moduleId");
    }

    @Override
    public String runJavaScriptFile(String filePath, boolean async, boolean reload, Context ctx) throws Exception {
        return jsExecuter.runJavaScriptFile(filePath, async, reload, ctx);
    }

    @Override
    public String runJavaScript(String script, Context ctx) throws Exception {
        return jsExecuter.runJavaScript(script, ctx);
    }

    @Override
    public long generateUniqueId(Context ctx) throws Exception {
//        if (ctx == null) ctx = EngineFactory.createContext(this);
        return idGenerator.nextId(ctx);
    }

    @Override
    public KVStorage getKVStorageForApplication(String id) throws Exception {
        Context ctx = EngineFactory.createContext(this);

        return new KVStorageImpl("A"+id, ctx);
    }

    @Override
    public KVStorage getKVStorageForIntegration(String id) throws Exception {
        Context ctx = EngineFactory.createContext(this);

        return new KVStorageImpl("I"+id, ctx);
    }

    @Override
    public KVStorage getKVStorageForClient(String id) throws Exception {
        Context ctx = EngineFactory.createContext(this);

        return new KVStorageImpl("C"+id, ctx);
    }

    @Override
    public FileStorage getFileStorageForApplication(String id) throws Exception {
        Context ctx = EngineFactory.createContext(this);

        return new FileStorageImpl(
                id,
                config.getProperty(EngineFactory.FileStorage_PrivateRootPath),
                config.getProperty(EngineFactory.FileStorage_PublicRootPath),
                config.getProperty(EngineFactory.FileStorage_PublicRootUrl),
                ctx
        );
    }

    @Override
    public FileStorage getFileStorageForIntegration(String id) throws Exception {
        Context ctx = EngineFactory.createContext(this);

        return new FileStorageImpl(
                id,
                config.getProperty(EngineFactory.FileStorage_PrivateRootPath),
                config.getProperty(EngineFactory.FileStorage_PublicRootPath),
                config.getProperty(EngineFactory.FileStorage_PublicRootUrl),
                ctx
        );
    }

    @Override
    public FileStorage getFileStorageForClient(String id) throws Exception {
        Context ctx = EngineFactory.createContext(this);

        return new FileStorageImpl(
                id,
                config.getProperty(EngineFactory.FileStorage_PrivateRootPath),
                config.getProperty(EngineFactory.FileStorage_PublicRootPath),
                config.getProperty(EngineFactory.FileStorage_PublicRootUrl),
                ctx
        );
    }

    @Override
    public DBStorage getDBStorageForApplication(String id) throws Exception {
        Context ctx = EngineFactory.createContext(this);

        if (EngineFactory.DBType_JDBC.equals(dbType))
            return new SQLDBStorageImpl(id, ctx);
        else
            throw new PhusionException("CONF_NONE", "Failed to get DB Storage because of missing "+EngineFactory.DB_Type);
    }

    @Override
    public DBStorage getDBStorageForIntegration(String id) throws Exception {
        Context ctx = EngineFactory.createContext(this);

        if (EngineFactory.DBType_JDBC.equals(dbType))
            return new SQLDBStorageImpl("I"+id, ctx);
        else
            throw new PhusionException("CONF_NONE", "Failed to get DB Storage because of missing "+EngineFactory.DB_Type);
    }

    @Override
    public DBStorage getDBStorageForClient(String id) throws Exception {
        Context ctx = EngineFactory.createContext(this);

        if (EngineFactory.DBType_JDBC.equals(dbType))
            return new SQLDBStorageImpl("C"+id, ctx);
        else
            throw new PhusionException("CONF_NONE", "Failed to get DB Storage because of missing "+EngineFactory.DB_Type);
    }

    @Override
    public HttpClient createHttpClient() throws Exception {
        boolean trustAnyHttpsCertificate = ! Boolean.parseBoolean(config.getProperty(EngineFactory.HttpClient_CheckHTTPSCerts, "true"));
        Context ctx = EngineFactory.createContext(this);
        return new HttpClientImpl(ctx, trustAnyHttpsCertificate, true);
    }

    @Override
    public void registerHttpServer(String path, HttpServer server, DataObject config, Context ctx) throws Exception {
        if (ctx == null) ctx = EngineFactory.createContext(this);
        ctx.setContextInfo("path", path);
        HttpServerManager.registerHttpServer(path, server, config, ctx);
        ctx.removeContextInfo("path");
    }

    @Override
    public void registerHttpServer(String path, HttpServer server, Context ctx) throws Exception {
        registerHttpServer(path, server, null, ctx);
    }

    @Override
    public void unregisterHttpServer(String path, Context ctx) throws Exception {
        if (ctx == null) ctx = EngineFactory.createContext(this);
        ctx.setContextInfo("path", path);
        HttpServerManager.unregisterHttpServer(path, ctx);
        ctx.removeContextInfo("path");
    }

    @Override
    public boolean doesHttpServerExist(String path) {
        return HttpServerManager.doesHttpServerExist(path);
    }

    @Override
    public void scheduleTask(String taskId, ScheduledTask task, String cron, boolean clustered, Context ctx) throws Exception {
        if (ctx == null) ctx = EngineFactory.createContext(this);
        if (scheduler == null) {
            throw new PhusionException("SCH_NONE", "Failed to schedule task", "taskId="+taskId, ctx);
        }

        ctx.setContextInfo("taskId", taskId);

        scheduler.scheduleTask(taskId, cron, ctx);
        TaskManager.addTask(taskId, task, clustered, ctx);

        ctx.removeContextInfo("taskId");
    }

    @Override
    public void scheduleTask(String taskId, ScheduledTask task, String cron, Context ctx) throws Exception {
        scheduleTask(taskId, task, cron, true, ctx);
    }

    @Override
    public void scheduleTask(String taskId, ScheduledTask task, Date startTime, long intervalInSeconds, long repeatCount, boolean clustered, Context ctx) throws Exception {
        if (ctx == null) ctx = EngineFactory.createContext(this);
        if (scheduler == null) {
            throw new PhusionException("SCH_NONE", "Failed to schedule task", "taskId="+taskId, ctx);
        }

        ctx.setContextInfo("taskId", taskId);

        scheduler.scheduleTask(taskId, startTime, intervalInSeconds, repeatCount, ctx);
        TaskManager.addTask(taskId, task, clustered, ctx);

        ctx.removeContextInfo("taskId");
    }

    @Override
    public void scheduleTask(String taskId, ScheduledTask task, Date startTime, long intervalInSeconds, long repeatCount, Context ctx) throws Exception {
        scheduleTask(taskId, task, startTime, intervalInSeconds, repeatCount, false, ctx);
    }

    @Override
    public void scheduleTask(String taskId, ScheduledTask task, long intervalInSeconds, long repeatCount, Context ctx) throws Exception {
        scheduleTask(taskId, task, null, intervalInSeconds, repeatCount, false, ctx);
    }

    @Override
    public void scheduleTask(String taskId, ScheduledTask task, long intervalInSeconds, Context ctx) throws Exception {
        scheduleTask(taskId, task, null, intervalInSeconds, 0, false, ctx);
    }

    @Override
    public void removeScheduledTask(String taskId, Context ctx) throws Exception {
        if (ctx == null) ctx = EngineFactory.createContext(this);
        ctx.setContextInfo("taskId", taskId);

        scheduler.removeScheduledTask(taskId, ctx);
        TaskManager.removeTask(taskId, ctx);

        ctx.removeContextInfo("taskId");
    }

    @Override
    public void clearAllScheduledTasks(Context ctx) throws Exception {
        if (ctx == null) ctx = EngineFactory.createContext(this);

        scheduler.clearAllTasks(ctx);
        TaskManager.clearAllTasks(ctx);

        ctx.logInfo(_position, "All tasks removed");
    }

    @Override
    public boolean doesTaskExist(String taskId) {
        return TaskManager.doesTaskExist(taskId);
    }

    @Override
    public String registerApplication(String applicationId, String moduleId, String appClassName, DataObject config, Context ctx) throws Exception {
        Application app = (Application) moduleManager.createClassInstance(moduleId, appClassName, ctx);
        app.setId(applicationId);

        if (ctx == null) ctx = EngineFactory.createContext(this);

        if (applicationId==null || applicationId.length()==0) {
            throw new PhusionException("APP_NOID", "Failed to register application", ctx);
        }
        ctx.setContextInfo("applicationId", app.getId());

        appManager.registerApplication(app, config, ctx);

        ctx.removeContextInfo("applicationId");
        return applicationId;
    }

    @Override
    public String registerApplication(String applicationId, String appClassName, DataObject config, Context ctx) throws Exception {
        // Use default ClassLoader to create the Application class
        Application app = (Application) Class.forName(appClassName).newInstance();
        app.setId(applicationId);

        if (ctx == null) ctx = EngineFactory.createContext(this);

        if (applicationId==null || applicationId.length()==0) {
            throw new PhusionException("APP_NOID", "Failed to register application", ctx);
        }
        ctx.setContextInfo("applicationId", app.getId());

        appManager.registerApplication(app, config, ctx);

        ctx.removeContextInfo("applicationId");
        return applicationId;
    }

    @Override
    public void removeApplication(String applicationId, Context ctx) throws Exception {
        if (ctx == null) ctx = EngineFactory.createContext(this);
        ctx.setContextInfo("applicationId", applicationId);
        appManager.removeApplication(applicationId, ctx);
        ctx.removeContextInfo("applicationId");
    }

    @Override
    public Application getApplication(String applicationId) throws Exception {
        return appManager.getApplication(applicationId);
    }

    @Override
    public ExecStatus getApplicationStatus(String applicationId) {
        Application app = appManager.getApplication(applicationId);
        if (app == null) return ExecStatus.None;
        else return app.getStatus();
    }

    @Override
    public void registerIntegration(String integrationId, IntegrationDefinition idef, DataObject config, Context ctx) throws Exception {
        registerIntegration(integrationId, null, idef, config, ctx);
    }

    @Override
    public void registerIntegration(String integrationId, String clientId, IntegrationDefinition idef, DataObject config, Context ctx) throws Exception {
        if (ctx == null) ctx = EngineFactory.createContext(this);
        ctx.setContextInfo("integrationId", integrationId);
        itManager.registerIntegration(integrationId, clientId, idef, config, ctx);
        ctx.removeContextInfo("integrationId");
    }

    @Override
    public void removeIntegration(String integrationId, Context ctx) throws Exception {
        if (ctx == null) ctx = EngineFactory.createContext(this);
        ctx.setContextInfo("integrationId", integrationId);
        itManager.removeIntegration(integrationId, ctx);
        ctx.removeContextInfo("integrationId");
    }

    @Override
    public ExecStatus getIntegrationStatus(String integrationId) {
        return itManager.getIntegrationStatus(integrationId);
    }

    @Override
    public boolean evaluateCondition(DataObject data) throws Exception {
        ConditionEvaluator eval = new ConditionEvaluator(data.getString());
        JSONObject obj = data.getJSONObject();
        return eval.evaluate(new DataObject(obj.getJSONObject("msg")),
                new DataObject(obj.getJSONObject("config")));
    }

    @Override
    public Integration getIntegration(String integrationId) {
        return itManager.getIntegration(integrationId);
    }

    @Override
    public boolean evaluateCondition(String condition, String msg, String config) throws Exception {
        ConditionEvaluator eval = new ConditionEvaluator(condition);
        return eval.evaluate(new DataObject(msg), new DataObject(config));
    }

}
