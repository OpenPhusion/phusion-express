package cloud.phusion.express.util;

import cloud.phusion.DataObject;
import cloud.phusion.EngineFactory;
import cloud.phusion.express.ExpressService;

import java.util.HashMap;
import java.util.Map;

public class ErrorResponse {

    private static Map<String, String> codes = new HashMap<>();

    static {
        codes.put("SYS_ERR", "Unidentified system error");
        codes.put("BAD_REQ_URL", "The HTTP method and url combination is not supported");
        codes.put("BAD_REQ_PARAM", "The param is missing, or has invalid data");
        codes.put("BAD_REQ_BODY", "The HTTP body is not parsable, or has invalid data");
        codes.put("BAD_REQ_TIME", "Not valid time format ("+ EngineFactory.DATETIME_FORMAT +")");
        codes.put("BAD_ENTITY", "The entity has some problem");
        codes.put("BAD_ID", "Should be sequence of (no more than 50) digits, letters, underscores");
        codes.put("BAD_MODULE", "Should be sequence of (no more than 50) digits, letters, dashes, underscores, points");
        codes.put("NOT_FOUND", "The entity is not found");
        codes.put("OP_NONE", "Operation not supported");
        codes.put("OP_ERR", "Operation can not be performed");
        codes.put("NOT_LOGIN", "Not authenticated");
        codes.put("BAD_LOGIN", "User name or password is wrong");
        codes.put("NO_PRIV", "No privilege to perform the action");
    }

    public static DataObject compose(String code) {
        return compose(code, null);
    }

    public static DataObject compose(String code, String data) {
        StringBuilder result = new StringBuilder();
        result.append("{\"error\":{");
        result.append("\"code\":\"").append(code).append("\",");
        result.append("\"msg\":\"").append(codes.get(code)).append("\"");
        if (data != null) result.append(",\"data\":").append(data);
        result.append("}}");

        return new DataObject(result.toString());
    }

}
