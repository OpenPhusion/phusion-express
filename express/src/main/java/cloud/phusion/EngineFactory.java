package cloud.phusion;

import cloud.phusion.express.ExpressContext;
import cloud.phusion.express.ExpressEngine;

import java.util.Properties;

public class EngineFactory {
    public static final String DATETIME_FORMAT = "yyyy-MM-dd HH:mm:ss";

    // Configuration Property Names

    // Engine ID: dataCenterId + workerId should be unique globally
    public final static String Cluster_DataCenter = "cluster.dataCenterId";
    public final static String Cluster_Worker = "cluster.workerId";

    public final static String HttpServer_Enabled = "http.server.enabled";
    public final static String HttpServer_Port = "http.server.port";
    public final static String HttpServer_MaxFileSize = "http.server.maxFileSize"; // Max POST size in bytes
    public final static String HttpClient_CheckHTTPSCerts = "http.client.checkHTTPSCerts";

    /*
    Cluster mode of the scheduling service:
    - Use Redis to collaborate the engines in the cluster, so Redis server is required.
      Each engine schedules tasks independently. If one acquired the Redis lock, it has the right to run the task,
      and the others simply give up.
      Before trying to acquire the lock, each engine waits for a random period of time for some kind of load balancing.
      If a task needs to run very frequently, say once in a second, it should not run in cluster mode.
    - Each host in the cluster should sync its system clock with the public time service.
    - Random range: the max length (in milliseconds) of period to wait randomly before running the task.
      It should be 10 times larger than the clock difference between hosts, and the default is 1000ms.
    - Lock time: the interval (in milliseconds) to keep the lock, that is to keep others to run the task.
      It should be 10 times large than randomRange, and the default is 10000ms.
    */
    public final static String Scheduler_Enabled = "scheduler.enabled";
    public final static String Scheduler_ThreadCount = "scheduler.threadCount";
    public final static String Scheduler_Clustered = "scheduler.cluster.enabled";
    public final static String Scheduler_RandomRange = "scheduler.cluster.randomRange";
    public final static String Scheduler_LockTime = "scheduler.cluster.lockTime";

    /*
    Root paths of the file system to host the file storage. The path should not end with "/".
    - Root for the Jar files.
    - Root for the JavaScript files.
    - Root for private files.
    - Root and Web address for public files.
    */
    public final static String Module_JavaFileRootPath = "module.java.fileRootPath";
    public final static String Module_JavaScriptFileRootPath = "module.javascript.fileRootPath";
    public final static String FileStorage_PrivateRootPath = "file.storage.privateRootPath";
    public final static String FileStorage_PublicRootPath = "file.storage.publicRootPath";
    public final static String FileStorage_PublicRootUrl = "file.storage.publicRootUrl";

    // Redis server
    public final static String Redis_Host = "kv.storage.redis.host";
    public final static String Redis_Port = "kv.storage.redis.port";
    public final static String Redis_Auth = "kv.storage.redis.auth";
    public final static String Redis_Database = "kv.storage.redis.database";

    // Database server: MongoDB, or JDBC source
    public final static String DB_Type = "db.storage.type";
    public final static String DBType_JDBC = "jdbc";

    // JDBC database server with connection pooling
    public final static String JDBC_DriverClass = "db.storage.jdbc.driverClass";
    public final static String JDBC_Url = "db.storage.jdbc.url";
    public final static String JDBC_DBName = "db.storage.jdbc.dbName";
    public final static String JDBC_User = "db.storage.jdbc.user";
    public final static String JDBC_Password = "db.storage.jdbc.password";
    public final static String JDBC_MinPoolSize = "db.storage.jdbc.minPoolSize";
    public final static String JDBC_MaxPoolSize = "db.storage.jdbc.maxPoolSize";

    // Store transaction logs into database
    public final static String TRXLog_Target = "db.storage.trxLog.target"; // The storage ID. If empty, do not store logs
    public final static String TRXLog_EncodeMsg = "db.storage.trxLog.encodeMsg"; // boolean
    public final static String TRXLog_Table_Transaction = "transaction";
    public final static String TRXLog_Table_TransactionStep = "transaction_step";
    public final static String TRXLog_Table_TransactionStepInfo = "transaction_step_info";

    static {
        // Exception codes

        PhusionException.setCode("APP_NOID", "Application does not have an ID");
        PhusionException.setCode("APP_EXIST", "Application registered already");
        PhusionException.setCode("APP_NONE", "Application not registered");
        PhusionException.setCode("APP_RUN", "Application is running, the operation is not allowed");
        PhusionException.setCode("APP_OP", "Application operation failed");
        PhusionException.setCode("APP_STOP", "Application is not running");
        PhusionException.setCode("APP_NONE_STOP", "Application not registered, nor connected");
        PhusionException.setCode("APP_REL_IT", "Related integration is running, the operation is not allowed");
        PhusionException.setCode("APP_DEF_ERR", "Application definition is invalid");
        PhusionException.setCode("EP_NONE", "Endpoint is not found");
        PhusionException.setCode("EP_FAIL", "Failed to call endpoint");
        PhusionException.setCode("CONN_NONE", "Connection does not exist");
        PhusionException.setCode("CONN_NONE_STOP", "Connection does not exist, or is not connected");
        PhusionException.setCode("CONN_RUN", "Connection is connected, the operation is not allowed");

        PhusionException.setCode("SCH_NONE", "Scheduler does not exist");
        PhusionException.setCode("SCH_P_CRON", "Can not schedule tasks both periodically and by CRON");

        PhusionException.setCode("IT_NONE", "Integration not registered");
        PhusionException.setCode("IT_EXIST", "Integration registered already");
        PhusionException.setCode("IT_RUN", "Integration is running, the operation is not allowed");
        PhusionException.setCode("IT_STOP", "Integration is not running");
        PhusionException.setCode("IT_OP", "Integration operation failed");
        PhusionException.setCode("IT_EXEC", "Integration execution failed");
        PhusionException.setCode("IT_NONE_LOOP", "Not found ForEach loop");
        PhusionException.setCode("IT_NESTED_LOOP", "Nested ForEach loop");

        PhusionException.setCode("FS_NONE", "File path configuration is missing");
        PhusionException.setCode("FS_OP", "File operation failed");
        PhusionException.setCode("FILE_NONE", "File does not exist");
        PhusionException.setCode("FILE_NOT", "It is not a file");
        PhusionException.setCode("FILE_INVALID", "Invalid file path");

        PhusionException.setCode("KV_OP", "KVStorage operation failed");

        PhusionException.setCode("DB_PARAMS", "The number of parameters do not match the statement");
        PhusionException.setCode("DB_OP", "Database operation failed");

        PhusionException.setCode("JAR_NONE", "Java module does not exist");
        PhusionException.setCode("JAR_OP", "Java module operation failed");

        PhusionException.setCode("JS_NONE", "Javascript module does not exist");
        PhusionException.setCode("JS_OP", "Javascript module operation failed");

        PhusionException.setCode("HTTP_WEB_NONE", "Web server not started");
        PhusionException.setCode("HTTP_NONE", "HTTP server not registered");
        PhusionException.setCode("HTTP_EXIST", "HTTP server registered already");
        PhusionException.setCode("HTTP_OP", "Failed to handle HTTP request");
        PhusionException.setCode("HTTP_CLIENT", "Failed to access HTTP service");

        PhusionException.setCode("TASK_NONE", "Task not registered");
        PhusionException.setCode("TASK_EXIST", "Task registered already");
        PhusionException.setCode("TASK_OP", "Failed to execute task");
        PhusionException.setCode("TASK_OP_SCH", "Failed to schedule task");

        PhusionException.setCode("TYPE_NONE", "Data type is not supported");
        PhusionException.setCode("CONF_NONE", "Configuration is missing");
        PhusionException.setCode("SYS_CLOCK", "System clock is not reliable");
    }

    private static Engine engine = null; // Be a singleton

    public static Engine createEngine() throws Exception {
//        if (engine == null) engine = new ExpressEngine();
        engine = new ExpressEngine();
        return engine;
    }

    public static Engine createEngine(Properties props) throws Exception {
//        if (engine == null) engine = new ExpressEngine(props);
        engine = new ExpressEngine(props);
        return engine;
    }

    public static Context createContext(Engine engine) {
        return new ExpressContext(engine);
    }

    public static Context createContext() {
        return new ExpressContext(engine);
    }

}
