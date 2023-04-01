package cloud.phusion.express.application;

import cloud.phusion.DataObject;
import cloud.phusion.PhusionException;
import cloud.phusion.application.*;
import cloud.phusion.Context;
import cloud.phusion.ExecStatus;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class ApplicationManager {
    private static final String _position = ApplicationManager.class.getName();
    private Map<String, Application> applications;

    public ApplicationManager() {
        super();

        this.applications = new ConcurrentHashMap<String, Application>();
    }

    public void registerApplication(Application app, DataObject config, Context ctx) throws Exception {
        ctx.logInfo(_position, "Initializing and registering application",
                String.format("appClassName=%s, config=%s", app.getClass().getName(), config==null?"":config.getString()));

        String applicationId = app.getId();

        if (applications.containsKey(applicationId)) {
            throw new PhusionException("APP_EXIST", "Failed to register application", ctx);
        }

        try {
            app.init(config, ctx);
        } catch (Exception ex) {
            throw new PhusionException("APP_OP", "Failed to initialize application", ctx, ex);
        }
        applications.put(applicationId, app);

        ctx.logInfo(_position, "Application initialized and registered");
    }

    public void removeApplication(String applicationId, Context ctx) throws Exception {
        Application app = applications.get(applicationId);

        if (app != null) {
            if (app.getStatus() == ExecStatus.Running) {
                throw new PhusionException("APP_RUN", "Failed to remove application", ctx);
            }

            try {
                app.destroy(ctx);
            } catch (Exception ex) {
                throw new PhusionException("APP_OP", "Failed to destroy application", ctx, ex);
            }
            applications.remove(applicationId);
        } else
            throw new PhusionException("APP_NONE", "Failed to remove application", ctx);
    }

    public Application getApplication(String applicationId) {
        return applications.get(applicationId);
    }

}
