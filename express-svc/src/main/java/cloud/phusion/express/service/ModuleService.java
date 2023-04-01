package cloud.phusion.express.service;

import cloud.phusion.*;
import cloud.phusion.express.ExpressService;
import cloud.phusion.express.util.CommonCode;
import cloud.phusion.express.util.ServiceLogger;
import cloud.phusion.protocol.http.HttpRequest;
import cloud.phusion.storage.FileStorage;
import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONArray;
import com.alibaba.fastjson2.JSONObject;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import javax.xml.parsers.DocumentBuilderFactory;
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Set;

public class ModuleService {
    private static final String _position = ModuleService.class.getName();

    public static final String JAVAMODUEL_SYSTSEM = "system";
    public static final String POM_LOCATION = "module.java.pomFile"; // pom.xml For list the Java packages loaded beforehand
    public static final String CODE_OWNER_INTEGRATION = "integration";

    private static String pomFile;
    private static final String ROOT_PATH_JAR = "/"+ExpressService.JAR_PATH+"/";
    private static final String MODULE_FILE = "module.json";

    private static final String ROOT_PATH_CODE = "/"+ExpressService.CODE_PATH+"/";

    public static void init(String strPomFile) {
        pomFile = strPomFile;
    }

    public static void saveJavaModule(String moduleId, DataObject module, Context ctx) throws Exception {
        Engine engine = ctx.getEngine();

        String strModule = module.getString();
        if (strModule==null || strModule.length()==0) return;

        FileStorage fStorage = engine.getFileStorageForApplication(ExpressService.STORAGE_ID);
        String path = ROOT_PATH_JAR+moduleId+"/"+MODULE_FILE;

        fStorage.saveToFile(path, strModule.getBytes(StandardCharsets.UTF_8), ctx);
    }

    public static DataObject fetchJavaModule(String moduleId, boolean listJars, Context ctx) throws Exception {
        FileStorage storage = ctx.getEngine().getFileStorageForApplication(ExpressService.STORAGE_ID);
        return _fetchJavaModule(moduleId, listJars, storage, ctx);
    }

    public static boolean hasJavaModule(String moduleId, Context ctx) throws Exception {
        return _hasCodeOrModule(ROOT_PATH_JAR+moduleId, ctx);
    }

    public static boolean hasJar(String moduleId, String jar, Context ctx) throws Exception {
        return _hasCodeOrModule(ROOT_PATH_JAR+moduleId+"/"+jar, ctx);
    }

    public static boolean hasCode(String owner, String code, Context ctx) throws Exception {
        return _hasCodeOrModule(ROOT_PATH_CODE+owner+"/"+code, ctx);
    }

    private static boolean _hasCodeOrModule(String path, Context ctx) throws Exception {
        FileStorage storage = ctx.getEngine().getFileStorageForApplication(ExpressService.STORAGE_ID);
        return storage.doesFileExist(path, ctx);
    }

    private static String[] _moduleFile = new String[]{MODULE_FILE};

    public static DataObject _fetchJavaModule(String moduleId, boolean listJars, FileStorage storage, Context ctx) throws Exception {
        String path = ROOT_PATH_JAR+moduleId+"/"+MODULE_FILE;

        if (storage.doesFileExist(path, ctx)) {
            byte[] content = storage.readAllFromFile(path, ctx);
            if (content == null || content.length == 0) return null;

            JSONObject module = JSON.parseObject(new String(content, StandardCharsets.UTF_8));

            // Get all jar files
            String[] jars = storage.listFiles(ROOT_PATH_JAR+moduleId, ctx);
            jars = CommonCode.removeItemsFromArray(jars, _moduleFile);
            module.put("jars", jars);

            return new DataObject(module);
        }
        else return null;
    }

    public static DataObject fetchSystemJavaModule(Context ctx) {
        JSONObject module = new JSONObject();
        JSONArray packages = new JSONArray();
        module.put("module", JAVAMODUEL_SYSTSEM);
        module.put("packages", packages);

        try {
            DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
            Document doc = factory.newDocumentBuilder().parse(new File(pomFile));
            NodeList list = doc.getElementsByTagName("dependency");

            for (int i = 0; i < list.getLength(); i++) {
                NodeList elements = list.item(i).getChildNodes();
                JSONObject pkg = new JSONObject();

                for (int j = 0; j < elements.getLength(); j++) {
                    Node node = elements.item(j);
                    if (node.getNodeType() == Node.ELEMENT_NODE) {
                        Element element = (Element) node;
                        String tag = element.getTagName();
                        if (tag.equals("groupId")) pkg.put("group",element.getTextContent().trim());
                        else if (tag.equals("artifactId")) pkg.put("artifact",element.getTextContent().trim());
                        else if (tag.equals("version")) pkg.put("version",element.getTextContent().trim());
                    }
                }

                packages.add(pkg);
            }
        } catch (Exception ex) {
            ServiceLogger.error(_position, "Failed to read pom.xml", "traceId="+ctx.getId(), ex);
        }

        return new DataObject(module);
    }

    public static DataObject listJavaModules(Context ctx) throws Exception {
        return listJavaModules(false, ctx);
    }

    public static DataObject listJavaModules(boolean noSystemModule, Context ctx) throws Exception {
        FileStorage storage = ctx.getEngine().getFileStorageForApplication(ExpressService.STORAGE_ID);
        JSONArray result = new JSONArray();
        DataObject moduleObj = null;

        if (! noSystemModule) {
            moduleObj = fetchSystemJavaModule(ctx);
            if (moduleObj != null) result.add(moduleObj.getJSONObject());
        }

        String[] modules = storage.listFolders(ROOT_PATH_JAR, ctx);
        if (modules != null) {
            for (String moduleId : modules) {
                moduleObj = _fetchJavaModule(moduleId, true, storage, ctx);
                if (moduleObj != null) result.add(moduleObj.getJSONObject());
            }
        }

        return new DataObject(result);
    }

    public static void uploadJars(String module, HttpRequest req, Context ctx) throws Exception {
        String path = ROOT_PATH_JAR+module+"/";

        if (req.hasFiles()) {
            Set<String> files = req.getFileNames();
            FileStorage storage = ctx.getEngine().getFileStorageForApplication(ExpressService.STORAGE_ID);

            for (String file : files) {
                try (InputStream in = req.getFileContent(file)) {
                    // If the file is uploaded not in multi-part form, "file" will be empty string.
                    if (file.length() > 0) storage.saveToFile(path+file, in, ctx);
                    else {} // Do nothing, just to close the stream
                }
            }
        }
    }

    public static void loadJavaModule(String module, Context ctx) throws Exception {
        loadJavaModule(module, false, ctx);
    }

    public static void loadJavaModule(String module, boolean forceLoad, Context ctx) throws Exception {
        if (module==null || module.length()==0) return;

        Engine engine = ctx.getEngine();

        if (!forceLoad && engine.doesJavaModuleExist(module)) return;

        FileStorage storage = engine.getFileStorageForApplication(ExpressService.STORAGE_ID);

        String[] jars = storage.listFiles(ROOT_PATH_JAR+module, ctx);
        if (jars==null || jars.length==0) return;

        String path = module+"/";
        for (int i = 0; i < jars.length; i++) jars[i] = path + jars[i];

        engine.loadJavaModule(module, jars, ctx);
    }

    public static void unloadJavaModule(String module, Context ctx) throws Exception {
        ctx.getEngine().unloadJavaModule(module, ctx);
    }

    public static void removeJavaModule(String module, Context ctx) throws Exception {
        FileStorage storage = ctx.getEngine().getFileStorageForApplication(ExpressService.STORAGE_ID);
        storage.removeFile(ROOT_PATH_JAR+module, ctx);
    }

    public static void removeJar(String module, String jar, Context ctx) throws Exception {
        FileStorage storage = ctx.getEngine().getFileStorageForApplication(ExpressService.STORAGE_ID);
        storage.removeFile(ROOT_PATH_JAR+module+"/"+jar, ctx);
    }

    public static boolean installNodeJSModule(String module, Context ctx) throws Exception {
        Engine engine = ctx.getEngine();
        try {
            engine.installNodeJSModule(module, ctx);
        } catch (Exception ex) {
            ServiceLogger.error(_position, "Failed to install Node.js module", "module="+module+", traceId="+ctx.getId(), ex);
        }
        return engine.doesNodeJSModuleExist(module);
    }

    public static boolean uninstallNodeJSModule(String module, Context ctx) throws Exception {
        Engine engine = ctx.getEngine();
        try {
            engine.uninstallNodeJSModule(module, ctx);
        } catch (Exception ex) {
            ServiceLogger.error(_position, "Failed to uninstall Node.js module", "module="+module+", traceId="+ctx.getId(), ex);
        }
        return ! engine.doesNodeJSModuleExist(module);
    }

    public static DataObject listNodeJSModules(Context ctx) throws Exception {
        return ctx.getEngine().listNodeJSModules(ctx);
    }

    public static DataObject listCodes(Context ctx) throws Exception {
        return listCodes(null, ctx);
    }

    public static DataObject listCodes(String owner, Context ctx) throws Exception {
        if (CODE_OWNER_INTEGRATION.equalsIgnoreCase(owner)) return null;

        FileStorage storage = ctx.getEngine().getFileStorageForApplication(ExpressService.STORAGE_ID);
        StringBuilder result = new StringBuilder();

        if (owner==null || owner.length()==0) {
            String[] owners;
            owners = storage.listFolders(ROOT_PATH_CODE, ctx);
            owners = CommonCode.removeItemsFromArray(owners, new String[]{CODE_OWNER_INTEGRATION});

            result.append("{");
            boolean isFirst = true;

            if (owners != null) {
                for (String anOwner : owners) {
                    if (isFirst) isFirst = false;
                    else result.append(",");

                    result.append("\"").append(anOwner).append("\":");
                    _listCodeForOwner(result, anOwner, storage, ctx);
                }
            }

            result.append("}");
        }
        else {
            _listCodeForOwner(result, owner, storage, ctx);
        }

        return new DataObject(result.toString());
    }

    private static void _listCodeForOwner(StringBuilder result, String owner, FileStorage storage, Context ctx) throws Exception {
        FileStorage.FileProperties[] files = storage.listFilesWithProperties(ROOT_PATH_CODE+owner,ctx);
        result.append("[");

        if (files!=null && files.length>0) {
            boolean isFirst = true;
            SimpleDateFormat df = new SimpleDateFormat(EngineFactory.DATETIME_FORMAT);

            for (FileStorage.FileProperties file : files) {
                if (file.isFolder) continue;
                if (isFirst) isFirst = false;
                else result.append(",");

                String strDate = df.format(new Date(file.updateTime));

                result.append("{\"filename\":\"").append(file.name)
                        .append("\",\"size\":").append(file.size)
                        .append(",\"updateTime\":\"").append(strDate)
                        .append("\"}");
            }
        }

        result.append("]");
    }

    public static void removeCode(String owner, String filename, Context ctx) throws Exception {
        FileStorage storage = ctx.getEngine().getFileStorageForApplication(ExpressService.STORAGE_ID);
        String path = ROOT_PATH_CODE+owner+"/"+filename;
        if (storage.doesFileExist(path, ctx)) storage.removeFile(path, ctx);
    }

    public static void saveCode(String owner, String filename, DataObject code, Context ctx) throws Exception {
        Engine engine = ctx.getEngine();

        String strCode = code.getString();
        if (strCode == null) strCode = "";

        FileStorage fStorage = engine.getFileStorageForApplication(ExpressService.STORAGE_ID);
        String path = ROOT_PATH_CODE+owner+"/"+filename;

        fStorage.saveToFile(path, strCode.getBytes(StandardCharsets.UTF_8), ctx);
    }

    public static DataObject fetchCode(String owner, String filename, Context ctx) throws Exception {
        FileStorage storage = ctx.getEngine().getFileStorageForApplication(ExpressService.STORAGE_ID);
        String path = ROOT_PATH_CODE+owner+"/"+filename;

        if (storage.doesFileExist(path, ctx)) {
            byte[] content = storage.readAllFromFile(path, ctx);
            if (content == null || content.length == 0) return null;

            return new DataObject(new String(content, StandardCharsets.UTF_8));
        }
        else return null;
    }

}
