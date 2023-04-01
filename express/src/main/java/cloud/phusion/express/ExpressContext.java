package cloud.phusion.express;

import cloud.phusion.Context;
import cloud.phusion.Engine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class ExpressContext implements Context {
    private Engine engine;
    private static Logger logger = LoggerFactory.getLogger("PhusionEngine");
    private Map<String, String> contextInfo = new ConcurrentHashMap<>();
    private String strContextInfo = null;
    private String id = null;

    public ExpressContext(Engine engine) {
        super();
        this.engine = engine;
        try {
            if (engine != null) {
                id = "" + engine.generateUniqueId(null);
                contextInfo.put("traceId", id);
            }
        } catch (Exception ex) {
            logger.error("Failed to generate traceId", ex);
        }
    }

    @Override
    public Engine getEngine() {
        return engine;
    }

    @Override
    public void setContextInfo(String key, String info) {
        contextInfo.put(key, info);
        strContextInfo = null;
    }

    @Override
    public void removeContextInfo(String key) {
        contextInfo.remove(key);
        strContextInfo = null;
    }

    @Override
    public void clearContextInfo() {
        contextInfo.clear();
        strContextInfo = null;
    }

    @Override
    public String getContextInfo() {
        _prepareContextInfo();
        return strContextInfo;
    }

    @Override
    public String getId() {
        return id;
    }

    @Override
    public void logInfo(String position, String msg) {
        logger.info(_mergeContextAndLogInfo(position, msg, null));
    }

    @Override
    public void logInfo(String position, String msg, String data) {
        logger.info(_mergeContextAndLogInfo(position, msg, data));
    }

    @Override
    public void logError(String position, String msg) {
        logger.error(_mergeContextAndLogInfo(position, msg, null));
    }

    @Override
    public void logError(String position, String msg, String data) {
        logger.error(_mergeContextAndLogInfo(position, msg, data));
    }

    @Override
    public void logError(String position, String msg, Throwable t) {
        logger.error(_mergeContextAndLogInfo(position, msg, null), t);
    }

    @Override
    public void logError(String position, String msg, String data, Throwable t) {
        logger.error(_mergeContextAndLogInfo(position, msg, data), t);
    }

    private String _mergeContextAndLogInfo(String position, String msg, String data) {
        _prepareContextInfo();

        StringBuilder result = new StringBuilder();
        if (position!=null && position.length()==0) position = null;
        if (msg!=null && msg.length()==0) msg = null;
        if (data!=null && data.length()==0) data = null;

        if (position != null) result.append(position);
        if (msg != null) result.append(position==null ? "" : " ")
                                .append(msg)
                                .append(data!=null || strContextInfo.length()>0 ? ". " : "");

        if (strContextInfo.length() > 0) {
            result.append(strContextInfo).append(data==null ? "" : ", ");
        }
        if (data != null) result.append(data);

        return result.toString();
    }

    private void _prepareContextInfo() {
        if (strContextInfo != null) return;

        if (contextInfo.size() ==0 ) {
            strContextInfo = "";
            return;
        }

        StringBuilder result = new StringBuilder();
        Set<String> keys = contextInfo.keySet();
        boolean isFirst = true;

        for (String key : keys) {
            if (isFirst) isFirst = false;
            else result.append(", ");

            result.append(key).append("=").append(contextInfo.get(key));
        }

        strContextInfo = result.toString();
    }

}
