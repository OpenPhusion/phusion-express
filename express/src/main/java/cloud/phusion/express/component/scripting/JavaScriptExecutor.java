package cloud.phusion.express.component.scripting;

import cloud.phusion.Context;
import cloud.phusion.DataObject;
import cloud.phusion.PhusionException;
import cloud.phusion.integration.Transaction;
import com.eclipsesource.v8.*;
import org.apache.commons.io.FileUtils;

import java.io.*;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * To support ES6, the script must run in strict mode.
 *
 * All script must run in sync mode.
 */
public class JavaScriptExecutor {
    private static final String _position = JavaScriptExecutor.class.getName();

    /**
     * Each thread has a ThreadLocal JavaScript Runtime to avoid frequently create it.
     *
     * Map<String, Object> map = local.get();
     *
     * Map (key name MUST have a 1-char prefix !):
     * "n" -> NodeJS runtime;
     * "f{scriptId}" -> V8Object function of the script;
     * "t{scriptId}" -> Long timestamp when load the script;
     * "r" -> The module refresh function;
     * "c" -> Current context
     */
    private final static ThreadLocal<Map<String,Object>> local = new ThreadLocal<Map<String,Object>>(){
        @Override
        protected Map<String,Object> initialValue() {
            Map<String,Object> map = new HashMap<>();

            NodeJS node = NodeJS.createNodeJS();
            map.put("n", node);

            // The JavaScript to Java bridge function
            V8Function callback = new V8Function(node.getRuntime(), new JavaScriptBridge(map));
            node.getRuntime().add(JavaScriptBridge.JavaCallback_Func, callback);

            // The JavaScript to Java callback to return async results
            V8Function asyncCallback = new V8Function(node.getRuntime(), (receiver, parameters) -> {
                callbackResult.put(
                        parameters.getString(0),
                        parameters.getString(1)
                );
                return null;
            });
            node.getRuntime().add(JavaScriptBridge.JavaCallback_AsyncResult, asyncCallback);

            // J2V8 collapses in Windows ?
//            try {
//                Thread.sleep(500);
//            } catch (Exception ex) {ex.printStackTrace();}

            return map;
        }
    };

    // script_id -> reload_timestamp
    private static Map<String, Long> scripts = new ConcurrentHashMap<String, Long>();

    // script_id -> script_file
    private static Map<String, String> scriptFiles = new ConcurrentHashMap<String, String>();

    // callbackHanlde -> result returned by javascript function
    private static Map<String, String> callbackResult = new ConcurrentHashMap<>();
    private static int callbackHandleCount = 0;

    private static synchronized String _generateHandle() {
        callbackHandleCount ++;
        if (callbackHandleCount == Integer.MAX_VALUE) callbackHandleCount = 0;
        return ""+callbackHandleCount;
    }

    private String basePath;

    public JavaScriptExecutor() throws Exception {
        this(null);
    }

    public JavaScriptExecutor(String basePath) throws Exception {
        super();
        this.basePath = basePath;
    }

    private void _prepareClearModuleScript() throws Exception {
        File f = new File(basePath+"clear_module.js");

        // Create the script file if not exist
        if (! f.exists()) {
            FileUtils.write(f,
                    "exports.clear_module=" +
                            "function(f){try{" +
                            "delete require.cache[f];" +
                            "}catch(e){console.log('Error clear_module: '+e.message)}}",
                    "UTF-8"
            );
        }

        Map<String,Object> map = local.get();
        NodeJS node = (NodeJS) map.get("n");

        V8Object func = node.require(f);
        map.put("r", func);
    }

    private void _executeClearModuleScript(String script) throws Exception {
        Map<String,Object> map = local.get();

        V8Object func = (V8Object) map.get("r");
        if (func == null) {
            _prepareClearModuleScript();
            func = (V8Object) map.get("r");
        }

        func.executeJSFunction("clear_module", (new File(script)).getCanonicalPath());
    }

    /**
     * Lazy load the script.
     *
     * Renew the timestamp, each thread will check it to reload the script when using it.
     *
     * @param scriptId i.e. moduleId
     * @param script path to the .js file
     */
    public void loadScript(String scriptId, String script, Context ctx) throws Exception {
        scripts.put(scriptId, System.currentTimeMillis());

        String f = basePath==null ? script : basePath+script;
        scriptFiles.put(scriptId, f);

        ctx.logInfo(_position, "JavaScript module loaded (to lazy-load later)", "script="+script);
    }

    public boolean doesScriptExists(String scriptId) {
        return scripts.containsKey(scriptId);
    }

    public void unloadScript(String scriptId, Context ctx) throws Exception {
        scripts.remove(scriptId);

        _executeClearModuleScript(scriptFiles.get(scriptId));
        scriptFiles.remove(scriptId);

        Map<String,Object> map = local.get();
        V8Object func = (V8Object) map.get("f"+scriptId);

        if (func != null) func.release();
        map.remove("f"+scriptId);
        map.remove("t"+scriptId);

        ctx.logInfo(_position, "JavaScript module unloaded");
    }

    private V8Object _getScript(String scriptId, Context ctx) throws Exception {
        Map<String,Object> map = local.get();
        V8Object currentFunc = (V8Object) map.get("f"+scriptId);
        Long currentTimestamp = (Long) map.get("t"+scriptId);

        Long refreshTimestamp = scripts.get(scriptId);

        if (refreshTimestamp==null || currentTimestamp==null || currentTimestamp<refreshTimestamp) {
            // Load the script

            if (currentFunc != null) currentFunc.release();

            NodeJS node = (NodeJS) map.get("n");
            File f = new File(scriptFiles.get(scriptId));
            try {
                currentFunc = node.require(f);
            } catch (Exception ex) {
                throw new PhusionException("JS_OP", "Failed to node.require script", ctx, ex);
            }

            map.put("f"+scriptId, currentFunc);
            map.put("t"+scriptId, System.currentTimeMillis());
        }

        if (currentFunc == null) throw new PhusionException("JS_NONE", "Failed to physically load script", ctx);
        else return currentFunc;
    }

    private Context _getContext() {
        Map<String,Object> map = local.get();
        return (Context) map.get("c");
    }

    private void _putContext(Context c) {
        Map<String,Object> map = local.get();
        map.put("c", c);
    }

    private void _popContext() {
        Map<String,Object> map = local.get();
        map.remove("c");
    }

    /**
     * The Transaction object is serialized into JSON string, and passed to the JavaScript Runtime.
     * After execution of the script, deserialize the JSON string into Transaction object.
     */
    public void runScriptWithinTransaction(String scriptId, Transaction trx) throws Exception {
        runScriptWithinTransaction(scriptId, trx, false);
    }

    public void runScriptWithinTransaction(String scriptId, Transaction trx, boolean async) throws Exception {
        if (trx == null) return;
        Context ctx = trx.getContext();

        try {
            V8Object func = _getScript(scriptId, ctx);
            _putContext(ctx);

            String in = trx.toJSONString();
            String out;

            if (async) {
                String handle = _generateHandle();

                Map<String,Object> map = local.get();
                NodeJS node = (NodeJS) map.get("n");
                func.executeJSFunction(JavaScriptBridge.JavascriptTransaction_Func, in, handle);
                while (node.handleMessage()) {}

                out = callbackResult.get(handle);
                callbackResult.remove(handle);
            }
            else
                out = (String) func.executeJSFunction(JavaScriptBridge.JavascriptTransaction_Func, in);

            if (out != null && out.length() > 0) trx.updateFromJSONString(out);
//        } catch (Exception ex) {
//            throw new PhusionException("JS_OP", "Failed to run script", ctx, ex);
        } finally {
            _popContext();
        }
    }

    public String runJavaScriptFile(String filePath, boolean async, boolean reload, Context ctx) throws Exception {
        Map<String,Object> map = local.get();

        try {
            map.put("c", ctx);
            NodeJS node = (NodeJS) map.get("n");
            String f = basePath==null ? filePath : basePath+filePath;

            if (reload) _executeClearModuleScript(f);

            V8Object func = node.require(new File(f));
            String out;

            if (async) {
                String handle = _generateHandle();

                func.executeJSFunction(JavaScriptBridge.Javascript_Func, handle);
                while (node.handleMessage()) {}

                out = callbackResult.get(handle);
                callbackResult.remove(handle);
            }
            else
                out = (String) func.executeJSFunction(JavaScriptBridge.Javascript_Func);

            func.release();
            return out;
//        } catch (Exception ex) {
//            throw new PhusionException("JS_OP", "Failed to run script", ctx, ex);
        } finally {
            map.remove("c");
        }
    }

    public String runJavaScript(String script, Context ctx) throws Exception {
        try {
            Map<String,Object> map = local.get();
            NodeJS node = (NodeJS) map.get("n");

            return node.getRuntime().executeStringScript(script);
//        } catch (Exception ex) {
//            throw new PhusionException("JS_OP", "Failed to run script", ctx, ex);
        } finally {
        }
    }

    private static boolean isWindows = System.getProperty("os.name").toLowerCase().startsWith("windows");

    public static void installNodeModule(String nodejsModule, Context ctx) throws Exception {
        String cmd = "npm install "+nodejsModule+" --save-prod --save-exact";
        cmd = (isWindows ? "cmd.exe /c " : "sh -c ") + cmd;

        ctx.logInfo(_position, "Installing Node.js module", "command="+cmd);

        Process proc = Runtime.getRuntime().exec(cmd);

        new BufferedReader(new InputStreamReader(proc.getInputStream(), Charset.defaultCharset()))
                .lines().forEach(System.out::println);

        boolean succ = proc.waitFor(300, TimeUnit.SECONDS);
        if (succ) succ = proc.exitValue() == 0;

        if (succ)
            ctx.logInfo(_position, "Node.js module installed");
        else
            throw new PhusionException("JS_OP", "Failed to install node.js module", ctx);
    }

    public static void uninstallNodeModule(String nodejsModule, Context ctx) throws Exception {
        String cmd = "npm uninstall "+nodejsModule+" --save-prod";
        cmd = (isWindows ? "cmd.exe /c " : "sh -c ") + cmd;

        ctx.logInfo(_position, "Uninstalling Node.js module", "command="+cmd);

        Process proc = Runtime.getRuntime().exec(cmd);

        new BufferedReader(new InputStreamReader(proc.getInputStream(), Charset.defaultCharset()))
                .lines().forEach(System.out::println);

        boolean succ = proc.waitFor(300, TimeUnit.SECONDS);
        if (succ) succ = proc.exitValue() == 0;

        if (succ)
            ctx.logInfo(_position, "Node.js module uninstalled");
        else
            throw new PhusionException("JS_OP", "Failed to uninstall node.js module", ctx);
    }

    public static boolean doesNodeJSModuleExist(String nodejsModule) {
        String cmd = "npm list "+nodejsModule;
        cmd = (isWindows ? "cmd.exe /c " : "sh -c ") + cmd;

        try {
            Process proc = Runtime.getRuntime().exec(cmd);

            ByteArrayOutputStream out = new ByteArrayOutputStream();

            try (PrintStream ps = new PrintStream(out, true, "UTF-8")) {
                new BufferedReader(new InputStreamReader(proc.getInputStream(), Charset.defaultCharset()))
                        .lines().forEach(ps::print);
            }

            boolean succ = proc.waitFor(30, TimeUnit.SECONDS);
            if (succ) succ = proc.exitValue() == 0;

            String output = out.toString("UTF-8");
            out.close();

//            System.out.println("Output: "+output);

            if (succ) return output.contains(nodejsModule);
            else return false;
        } catch (Exception ex) {
            return false;
        }
    }

    public static DataObject listNodeJSModules() {
        String cmd = "npm ls";
        cmd = (isWindows ? "cmd.exe /c " : "sh -c ") + cmd;

        StringBuilder result = new StringBuilder();
        result.append("[");

        try {
            Process proc = Runtime.getRuntime().exec(cmd);

            ByteArrayOutputStream out = new ByteArrayOutputStream();

            try (PrintStream ps = new PrintStream(out, true, "UTF-8")) {
                new BufferedReader(new InputStreamReader(proc.getInputStream(), Charset.defaultCharset()))
                        .lines().forEach(str -> ps.println(str));
            }

            boolean succ = proc.waitFor(30, TimeUnit.SECONDS);
            if (succ) succ = proc.exitValue() == 0;

            String output = out.toString("UTF-8");
            out.close();

//            System.out.println("Output: "+output);

            if (succ) {
                int pos = 0;
                int midPos = 0;
                int nextPos = 0;
                boolean hasItems = false;

                while (pos >= 0) {
                    nextPos = output.indexOf("\n", pos);

                    String line = output.substring(pos, nextPos);
                    midPos = line.indexOf(' ');
                    if (midPos > 0) {
                        line = line.substring(midPos+1);
                        midPos = line.indexOf('/');
                        if (midPos > 0) line = line.substring(midPos+1);

                        midPos = line.indexOf('@');
                        if (midPos > 0) {
                            if (hasItems) result.append(",");
                            else hasItems = true;

                            result.append("{\"module\":\"").append(line.substring(0,midPos));

                            String version = line.substring(midPos+1);
                            midPos = version.indexOf(' ');
                            version = midPos<0 ? version.trim() : version.substring(0,midPos);
                            result.append("\",\"version\":\"").append(version).append("\"}");
                        }
                    }

                    pos = nextPos<0 ? nextPos : (nextPos+1);
                }
            }
        } catch (Exception ex) {
        }

        result.append("]");

        return new DataObject(result.toString());
    }

}
