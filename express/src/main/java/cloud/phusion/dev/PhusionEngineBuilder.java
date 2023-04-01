package cloud.phusion.dev;

import cloud.phusion.Engine;
import cloud.phusion.EngineFactory;

import java.util.Properties;

public class PhusionEngineBuilder {
    private boolean needWebserver = false;
    private int webServerPort = 9900;

    private boolean needScheduler = false;

    private boolean needFileStorage = false;
    private String fsBase = PhusionEngineBuilder.class.getClassLoader().getResource("").getPath();
    private String fsScriptPath = fsBase + "FileStorage/javascript/";
    private String fsPrivatePath = fsBase + "FileStorage/private";
    private String fsPublicPath = fsBase + "FileStorage/public";
    private String fsPublicUrl = "https://fake.com/filestorage";

    private boolean needKVStorage = false;
    private boolean needDBStorage = false;

    private String dbType = null;
    private String dbIp = null;
    private int dbPort = 0;
    private String dbName = null;
    private String dbUser = null;
    private String dbPassword = null;

    private String kvIp = null;
    private int kvPort = 0;
    private String kvDB = null;
    private String kvAuth = null;

    public PhusionEngineBuilder needWebServer(boolean needed) {
        needWebserver = needed;
        return this;
    }

    public PhusionEngineBuilder needWebServer(boolean needed, int port) {
        needWebserver = needed;
        webServerPort = port;
        return this;
    }

    public PhusionEngineBuilder needScheduler(boolean needed) {
        needScheduler = needed;
        return this;
    }

    public PhusionEngineBuilder needFileStorage(boolean needed) {
        needFileStorage = needed;
        return this;
    }

    public PhusionEngineBuilder needFileStorage(boolean needed, String privatePath, String publicPath, String url) {
        needFileStorage = needed;
        if (privatePath != null) fsPrivatePath = privatePath;
        if (publicPath != null) fsPublicPath = publicPath;
        if (url != null) fsPublicUrl = url;
        return this;
    }

    public PhusionEngineBuilder needKVStorage(boolean needed) {
        needKVStorage = needed;
        return this;
    }

    public PhusionEngineBuilder needKVStorage(boolean needed, String ip, int port, String database, String auth) {
        needKVStorage = needed;
        this.kvIp = ip;
        this.kvPort = port;
        this.kvDB = database;
        this.kvAuth = auth;
        return this;
    }

    public PhusionEngineBuilder needDBStorage(boolean needed) {
        needDBStorage = needed;
        return this;
    }

    public PhusionEngineBuilder needDBStorage(boolean needed, String dbType, String ip, int port, String dbName, String user, String password) {
        needDBStorage = needed;
        this.dbType = dbType;
        this.dbIp = ip;
        this.dbPort = port;
        this.dbName = dbName;
        this.dbUser = user;
        this.dbPassword = password;
        return this;
    }

    public Engine done() throws Exception {
        Properties props = new Properties();

        if (needWebserver) {
            props.setProperty(EngineFactory.HttpServer_Enabled, "true");
            props.setProperty(EngineFactory.HttpServer_Port, ""+webServerPort);
        }

        if (needScheduler)
            props.setProperty(EngineFactory.Scheduler_Enabled, "true");

        if (needFileStorage) {
            System.out.println("-------------->"+fsPrivatePath);
            if (fsPrivatePath!=null) props.setProperty(EngineFactory.FileStorage_PrivateRootPath, fsPrivatePath);
            if (fsPublicPath!=null) props.setProperty(EngineFactory.FileStorage_PublicRootPath, fsPublicPath);
            if (fsPublicUrl!=null) props.setProperty(EngineFactory.FileStorage_PublicRootUrl, fsPublicUrl);
        }

        if (needKVStorage) {
            if (kvIp != null) {
                props.setProperty(EngineFactory.Redis_Host, kvIp);
                props.setProperty(EngineFactory.Redis_Port, ""+kvPort);
                props.setProperty(EngineFactory.Redis_Database, kvDB);
                props.setProperty(EngineFactory.Redis_Auth, kvAuth);
            }
        }

        if (needDBStorage) {
            props.setProperty(EngineFactory.DB_Type, EngineFactory.DBType_JDBC);

            if (dbType == null) {
                props.setProperty(EngineFactory.JDBC_DriverClass, "org.h2.Driver");
                props.setProperty(EngineFactory.JDBC_Url, "jdbc:h2:mem:test;DATABASE_TO_UPPER=FALSE");
            }
            else if (dbType.equals("mysql")) {
                props.setProperty(EngineFactory.JDBC_DriverClass, "com.mysql.cj.jdbc.Driver");
                props.setProperty(EngineFactory.JDBC_Url, "jdbc:mysql://"+dbIp+":"+dbPort+"/"+dbName);
                props.setProperty(EngineFactory.JDBC_DBName, dbName);
                props.setProperty(EngineFactory.JDBC_User, dbUser);
                props.setProperty(EngineFactory.JDBC_Password, dbPassword);
            }
        }

        props.setProperty(EngineFactory.Module_JavaScriptFileRootPath, fsScriptPath);

        return EngineFactory.createEngine(props);
    }

}
