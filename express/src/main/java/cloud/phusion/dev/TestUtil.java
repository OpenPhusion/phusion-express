package cloud.phusion.dev;

import cloud.phusion.Context;
import cloud.phusion.DataObject;
import cloud.phusion.Engine;
import cloud.phusion.EngineFactory;
import cloud.phusion.application.Application;
import cloud.phusion.express.component.scripting.JavaScriptExecutor;
import cloud.phusion.integration.Transaction;

public class TestUtil {

    public static PhusionEngineBuilder buildPhusionEngine() {
        return new PhusionEngineBuilder();
    }

    public static TransactionBuilder buildTransaction() {
        return new TransactionBuilder();
    }

    public static ApplicationRegister registerApplication() {
        return new ApplicationRegister();
    }

    public static String callOutboundEndpoint(Engine engine, String applicationId, String endpointId, String incomingMessage) throws Exception {
        Application app = engine.getApplication(applicationId);

        DataObject result = app.callOutboundEndpoint(
                endpointId,
                getIntegrationId(applicationId),
                new DataObject(incomingMessage),
                EngineFactory.createContext(engine)
        );

        return result.getString();
    }

    public static String getConnectionId(String applicationId) {
        return "CONN_For_" + applicationId;
    }

    public static String getIntegrationId(String applicationId) {
        return "IT_For_" + applicationId;
    }

    public static void unregisterApplication(Engine engine, String applicationId) throws Exception {
        Application app = engine.getApplication(applicationId);
        Context ctx = EngineFactory.createContext(engine);

        app.stop(ctx);
        engine.removeApplication(applicationId, null);
    }

    public static void runScriptWithinTransaction(String scriptPath, Transaction trx) throws Exception {
        Context ctx = trx.getContext();
        Engine engine = ctx.getEngine();
        String module = "ScriptTest";

        if (! engine.doesJavaScriptModuleExist(module)) {
            engine.loadJavaScriptModule(module, scriptPath, ctx);
        }

        engine.runJavaScriptWithTransaction(module, trx);
    }

}
