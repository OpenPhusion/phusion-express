package cloud.phusion.express.integration;

import cloud.phusion.Context;
import cloud.phusion.DataObject;
import cloud.phusion.ScheduledTask;
import cloud.phusion.integration.Integration;
import cloud.phusion.integration.Transaction;

public class IntegrationStarter implements ScheduledTask {
    private static final String _position = IntegrationStarter.class.getName();

    private Integration it;

    public IntegrationStarter(Integration it) {
        super();
        this.it = it;
    }

    @Override
    public void run(String taskId, Context ctx) {
        String integrationId = null;
        String transactionId = null;

        try {
            Transaction trx = it.createInstance(ctx);
            integrationId = trx.getIntegrationId();
            transactionId = trx.getId();

            // This is a dedicated context. So the context info do not need to be removed after execution
            ctx.setContextInfo("integrationId", integrationId);
            ctx.setContextInfo("transactionId", transactionId);

            it.runInstance(trx);
        } catch (Exception ex) {
            ctx.logError(_position, "Failed to execute integration by IntegrationStarter.run", ex);
        }
    }
}
