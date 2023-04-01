package cloud.phusion.express.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ServiceLogger {
    private static Logger logger = LoggerFactory.getLogger("PhusionService");

    public static void info(String position, String msg) {
        logger.info(_mergeLogInfo(position, msg, null));
    }

    public static void info(String position, String msg, String data) {
        logger.info(_mergeLogInfo(position, msg, data));
    }

    public static void error(String position, String msg) {
        logger.error(_mergeLogInfo(position, msg, null));
    }

    public static void error(String position, String msg, String data) {
        logger.error(_mergeLogInfo(position, msg, data));
    }

    public static void error(String position, String msg, Throwable t) {
        logger.error(_mergeLogInfo(position, msg, null), t);
    }

    public static void error(String position, String msg, String data, Throwable t) {
        logger.error(_mergeLogInfo(position, msg, data), t);
    }

    private static String _mergeLogInfo(String position, String msg, String data) {
        StringBuilder result = new StringBuilder();
        if (position!=null && position.length()==0) position = null;
        if (msg!=null && msg.length()==0) msg = null;
        if (data!=null && data.length()==0) data = null;

        if (position != null) result.append(position);
        if (msg != null) result.append(position==null ? "" : " ")
                                .append(msg)
                                .append(data!=null ? ". " : "");

        if (data != null) result.append(data);

        return result.toString();
    }

}
