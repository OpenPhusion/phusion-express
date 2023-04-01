package cloud.phusion.express.service;

import cloud.phusion.Context;
import cloud.phusion.EngineFactory;
import cloud.phusion.ScheduledTask;
import cloud.phusion.express.component.storage.MessageListener;
import cloud.phusion.express.util.ServiceLogger;

public class ClusterServiceHandler implements ScheduledTask, MessageListener {
    private static final String _position = ClusterServiceHandler.class.getName();

    private Context msgCtx;
    private String msgChannel;

    public ClusterServiceHandler(String channel, Context ctx) {
        super();
        this.msgCtx = ctx;
        this.msgChannel = channel;
    }

    @Override
    public void run(String taskId, Context ctx) {
        try {
            ClusterService.heartbeat(ctx);
        } catch (Exception ex) {
            ServiceLogger.error(_position, "Heartbeat failed", "traceId="+ctx.getId(), ex);
        }
    }

    @Override
    public void onMessage(String channel, String message) {
        if (channel.endsWith(msgChannel)) { // The real channel name has some prefix
            String[] parts = message.split(",");
            String data = String.format("channel=%s, message=%s", channel, message);

            Context ctx = EngineFactory.createContext(msgCtx.getEngine()); // Start new context

            try {
                switch (parts[0]) {
                    case ClusterService.OBJECT_APPLICATION:
                        ApplicationService.performAction(parts[1], parts[2], ctx);
                        break;
                    case ClusterService.OBJECT_CONNECTION:
                        ConnectionService.performAction(parts[1], parts[2], ctx);
                        break;
                    case ClusterService.OBJECT_INTEGRATION:
                        IntegrationService.performAction(parts[1], parts[2], ctx);
                        break;
                    case ClusterService.OBJECT_USER:
                        UserService.performAction(parts[1], parts[2], ctx);
                        break;
                }

                ServiceLogger.info(_position, "Received message", data);
            } catch (Exception ex) {
                ServiceLogger.error(_position, "Failed to handle message", data, ex);
            }
        }
    }

}
