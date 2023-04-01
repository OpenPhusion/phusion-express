package cloud.phusion.dev;

import cloud.phusion.Context;
import cloud.phusion.DataObject;
import cloud.phusion.Engine;
import cloud.phusion.EngineFactory;
import cloud.phusion.application.Application;
import cloud.phusion.express.integration.IntegrationImpl;

public class ApplicationRegister implements IntegrationMocker {
    private String applicationId = null;
    private String endpoint = null;
    private String mock = null;
    private String applicationConfig = null;
    private String connectionConfig = null;
    private String applicationClass = null;
    private Engine engine = null;

    public ApplicationRegister setEngine(Engine engine) {
        this.engine = engine;
        return this;
    }

    public ApplicationRegister setApplicationId(String str) {
        this.applicationId = str;
        return this;
    }

    public ApplicationRegister setApplicationConfig(String str) {
        this.applicationConfig = str;
        return this;
    }

    public ApplicationRegister setConnectionConfig(String str) {
        this.connectionConfig = str;
        return this;
    }

    public ApplicationRegister setEndpointToTest(String str) {
        this.endpoint = str;
        return this;
    }

    public ApplicationRegister setIntegrationMockedResult(String str) {
        this.mock = str;
        return this;
    }

    public ApplicationRegister setApplicationClass(String str) {
        this.applicationClass = str;
        return this;
    }

    public void done() throws Exception {
        Context ctx = EngineFactory.createContext(engine);

        if (engine.getApplication(applicationId) != null) return;

        engine.registerApplication(applicationId, applicationClass, new DataObject(applicationConfig), ctx);
        Application app = engine.getApplication(applicationId);
        app.start(ctx);

        String connectionId = TestUtil.getConnectionId(applicationId);
        String integrationId = TestUtil.getIntegrationId(applicationId);

        app.createConnection(connectionId, new DataObject(connectionConfig), ctx);
        app.connect(connectionId, ctx);

        if (engine.getIntegration(integrationId) != null) return;

        engine.registerIntegration(integrationId, null, null, ctx);

        app.addEndpointForIntegration(endpoint, integrationId, connectionId, null);

        IntegrationImpl.setIntegrationMocker(this);
    }

    @Override
    public DataObject executeIntegration(String integrationId, DataObject msg) throws Exception {
        if (integrationId!=null && integrationId.equals(TestUtil.getIntegrationId(applicationId))) {
            System.out.println();
            System.out.println("Incoming message: " + msg.getString());
            System.out.println();

            return new DataObject(mock);
        }
        else throw new Exception("Bad integrationId: "+integrationId);
    }

}
