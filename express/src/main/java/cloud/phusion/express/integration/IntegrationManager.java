package cloud.phusion.express.integration;

import cloud.phusion.Context;
import cloud.phusion.DataObject;
import cloud.phusion.ExecStatus;
import cloud.phusion.PhusionException;
import cloud.phusion.integration.Integration;
import cloud.phusion.integration.IntegrationDefinition;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class IntegrationManager {
    private static final String _position = IntegrationManager.class.getName();

    private Map<String, Integration> integrations;

    public IntegrationManager() {
        super();

        this.integrations = new ConcurrentHashMap<>();
    }

    public void registerIntegration(String integrationId, String clientId, IntegrationDefinition idef, DataObject config, Context ctx) throws Exception {
        if (integrations.containsKey(integrationId))
            throw new PhusionException("IT_EXIST", "Failed to register integration", ctx);

        ctx.logInfo(_position, "Registering integration", "config="+(config==null?"":config.getString()));

        Integration it = new IntegrationImpl(idef);
        it.setId(integrationId);
        it.setClientId(clientId);
        it.init(config, ctx);

        integrations.put(integrationId, it);

        ctx.logInfo(_position, "Integration registered");
    }

    public void removeIntegration(String integrationId, Context ctx) throws Exception {
        Integration it = integrations.get(integrationId);

        if (it != null) {
            if (it.getStatus() == ExecStatus.Running)
                throw new PhusionException("IT_RUN", "Failed to remove integration", ctx);

            it.destroy(ctx);
            integrations.remove(integrationId);
        } else
            throw new PhusionException("IT_NONE", "Failed to remove integration", ctx);
    }

    public ExecStatus getIntegrationStatus(String integrationId) {
        Integration it = integrations.get(integrationId);
        if (it == null) return ExecStatus.None;
        else return it.getStatus();
    }

    public Integration getIntegration(String integrationId) {
        return integrations.get(integrationId);
    }

}
