package cloud.phusion.express.service;

import cloud.phusion.Context;
import cloud.phusion.DataObject;
import cloud.phusion.Engine;
import cloud.phusion.express.ExpressService;
import cloud.phusion.integration.Transaction;

import java.io.*;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class CodeRunner {
    public static final String CODE_TYPE_JS = "js";
    public static final String CODE_TYPE_NODE = "node.js";
    public static final String CODE_TYPE_DSL_I = "i.dsl.js";
    public static final String CODE_TYPE_DSL_C = "c.dsl.json";

    public static String getTypeFromFilename(String filename) {
        if (filename==null || filename.length()==0) return null;
        filename = filename.toLowerCase();

        if (filename.endsWith("."+CODE_TYPE_DSL_C)) return CODE_TYPE_DSL_C;
        if (filename.endsWith("."+CODE_TYPE_DSL_I)) return CODE_TYPE_DSL_I;
        if (filename.endsWith("."+CODE_TYPE_NODE)) return CODE_TYPE_NODE;
        if (filename.endsWith("."+CODE_TYPE_JS)) return CODE_TYPE_JS;

        return null;
    }

    public static String wrapCode(String type, String code) {
        if (type.equals(CODE_TYPE_DSL_I)) return _wrapCode(idslWrapCode, code);
        else if (type.equals(CODE_TYPE_JS)) return _wrapCode(jsWrapCode, code);
        else return code;
    }

    public static String unwrapCode(String type, String code) {
        if (type.equals(CODE_TYPE_DSL_I)) return _unwrapCode(code);
        else if (type.equals(CODE_TYPE_JS)) return _unwrapCode(code);
        else return code;
    }

    private static final String _wrapCodeBegin = "/*BEGIN*/";
    private static final String _wrapCodeEnd = "/*END*/";

    private static final String jsWrapCode =
            "(function(){\n/*BEGIN*//*END*/\n})();";

    private static final String idslWrapCode =
            "var def = require(\"phusion/IntegrationDef\");\n" +
                    "exports._run = function() {\n" +
                    "    try {\n" +
                    "        var result = def.IntegrationDefStart.\n/*BEGIN*//*END*/\n" +
                    "        if (typeof(result) != \"string\") throw new Error(\"Must end with done()\");\n" +
                    "        return result;\n" +
                    "    } catch(e) {\n" +
                    "        var str = e.name+\": \"+e.message;\n" +
                    "        if (str.indexOf(\"not a function\") > 0) str += \". Must start with in() or timer()\";\n" +
                    "        return str;\n" +
                    "    }\n" +
                    "};";

    private static String _wrapCode(String wrapCode, String code) {
        if (code == null) code = "";
        int pos = wrapCode.indexOf(_wrapCodeBegin) + _wrapCodeBegin.length();

        StringBuilder result = new StringBuilder();
        result.append(wrapCode.substring(0,pos)).append('\n')
                .append(code).append('\n')
                .append(wrapCode.substring(pos));
        return result.toString();
    }

    private static String _unwrapCode(String code) {
        if (code==null || code.length()==0) return null;

        int posBegin = code.indexOf(_wrapCodeBegin);
        if (posBegin < 0) return null;
        posBegin += _wrapCodeBegin.length() + 1; // 1 is for the '\n' char added in wrap code

        int posEnd = code.indexOf(_wrapCodeEnd, posBegin);
        if (posEnd < 0) return null;

        return code.substring(posBegin, posEnd - 1); // 1 is for the '\n' char added in wrap code
    }

    public static String runCode(String type, String code, String owner, String filename, boolean async, boolean reload, Context ctx) throws Exception {
        if (type==null || type.length()==0) type = getTypeFromFilename(filename);

        if (type.equals(CODE_TYPE_JS) || type.equals(CODE_TYPE_DSL_C)) {
            if (code == null || code.length() == 0) {
                if (owner==null || owner.length()==0 || filename==null || filename.length()==0) return null;

                DataObject codeObj = ModuleService.fetchCode(owner, filename, ctx);
                if (codeObj == null) return null;
                code = codeObj.getString();
            }

            if (type.equals(CODE_TYPE_JS)) return runJSCode(code, ctx);
            else return runCDSLCode(code, ctx);
        }
        else {
            if (owner==null || owner.length()==0 || filename==null || filename.length()==0) return null;

            if (owner.equals("integration")) return runIntegrationCodeFile(owner, filename, async, reload, ctx);
            else return runNodeJSCodeFile(owner, filename, async, reload, ctx);
        }
    }

    public static String runJSCode(String code, Context ctx) throws Exception {
        try {
            return ctx.getEngine().runJavaScript(code, ctx);
        } catch (Exception ex) {
            return _outputException(ex);
        }
    }

    public static String runNodeJSCodeFile(String owner, String filename, boolean async, boolean reload, Context ctx) throws Exception {
        try {
            return ctx.getEngine().runJavaScriptFile(owner+"/"+filename, async, reload, ctx);
        } catch (Exception ex) {
            return _outputException(ex);
        }
    }

    public static String runIntegrationCodeFile(String owner, String filename, boolean async, boolean reload, Context ctx) throws Exception {
        Engine engine = ctx.getEngine();
        String scriptId = ""+engine.generateUniqueId(ctx);

        if (reload && engine.doesJavaScriptModuleExist(scriptId))
            engine.unloadJavaScriptModule(scriptId, ctx);

        try {
            engine.loadJavaScriptModule(scriptId, owner+"/"+filename, ctx);

            Transaction trx = new Transaction(
                    filename.substring(0,filename.indexOf('.')),
                    "Tx-"+scriptId, "", ctx);

            engine.runJavaScriptWithTransaction(scriptId, trx, async);
            return trx.toJSONString();
        } catch (Exception ex) {
            return _outputException(ex);
        } finally {
            if (reload) engine.unloadJavaScriptModule(scriptId, ctx);
        }
    }

    public static String runCDSLCode(String code, Context ctx) throws Exception {
        return ""+ctx.getEngine().evaluateCondition(new DataObject(code));
    }

    private static String _outputException(Exception ex) throws Exception {
        try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            try (PrintStream ps = new PrintStream(out, true, "UTF-8")) {
                ex.printStackTrace(ps);
            }
            return out.toString("UTF-8");
        }
    }

    private static final String integrationWrapCode =
            "var bridge = require(\"phusion/JavaBridge\");\n" +
                    "\n" +
                    "exports._runTransaction = function(strTransaction) {\n" +
                    "    var trx = new bridge.Transaction(strTransaction);\n" +
                    "    var step = trx.getCurrentStep();\n" +
                    "\n" +
                    "    switch (step) {\n/*BEGIN*//*END*/" +
                    "    }\n" +
                    "\n" +
                    "    return trx.toString();\n" +
                    "};\n";

    private static final String integrationStepFunc =
            "    var msg = trx.getMessage();\n" +
                    "    var config = trx.getIntegrationConfig();\n" +
                    "\n" +
                    "    //TODO\n" +
                    "\n" +
                    "    trx.setMessage(msg);\n" +
                    "    // trx.moveToStep(\"...\");\n" +
                    "    // trx.moveToEnd();\n";

    public static String generateCodeForIntegration(List<String> javascriptSteps) {
        if (javascriptSteps == null) javascriptSteps = new ArrayList<>();

        int pos = integrationWrapCode.indexOf(_wrapCodeBegin);

        StringBuilder result = new StringBuilder();
        result.append(integrationWrapCode.substring(0,pos));

        for (String step : javascriptSteps) {
            result.append("        case \"").append(step).append("\": on")
                    .append(step.substring(0,1).toUpperCase()).append(step.substring(1))
                    .append("(trx); break;\n");
        }

        result.append(integrationWrapCode.substring(pos + _wrapCodeBegin.length() + _wrapCodeEnd.length()));

        for (String step : javascriptSteps) {
            result.append("\nfunction on")
                    .append(step.substring(0,1).toUpperCase()).append(step.substring(1))
                    .append("(trx) {\n")
                    .append(integrationStepFunc)
                    .append("}\n");
        }

        return result.toString();
    }

}
