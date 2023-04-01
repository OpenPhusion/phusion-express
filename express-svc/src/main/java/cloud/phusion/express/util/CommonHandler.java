package cloud.phusion.express.util;

import cloud.phusion.Context;
import cloud.phusion.DataObject;
import cloud.phusion.protocol.http.HttpRequest;
import cloud.phusion.protocol.http.HttpResponse;
import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONArray;
import com.alibaba.fastjson2.JSONObject;

import java.io.InputStream;
import java.util.*;
import java.util.regex.Pattern;

public class CommonHandler {
    private final static String _position = CommonHandler.class.getName();

    public static JSONObject parseDataToJSONObject(DataObject obj, HttpResponse resp, String errCode, Context ctx) {
        return parseDataToJSONObject(obj, resp, errCode, null, null, ctx);
    }

    public static JSONObject parseDataToJSONObject(DataObject obj, HttpResponse resp, String errCode, String entityIdStr, String entityId, Context ctx) {
        if (obj==null || obj.getString()==null || obj.getString().length()==0) {
            resp.setBody(ErrorResponse.compose(errCode,
                    entityIdStr==null ?
                    "{\"exception\":\"content is empty\"}" :
                    "{\"exception\":\"content is empty\",\""+entityIdStr+"\":\""+entityId+"\"}"
            ));
            return null;
        }

        try {
            return JSON.parseObject(obj.getString());
        } catch (Exception ex) {
            ServiceLogger.error(_position, "Failed to parse content", "errCode="+errCode+", traceId="+ctx.getId(), ex);

            StringBuilder data = new StringBuilder();
            String msg = ex.getMessage();
            int pos = msg.indexOf('\n');
            if (pos >= 0) msg = msg.substring(0,pos);
            data.append("{\"exception\":\"").append(ex.getClass().getName()).append(": ").append(msg).append("\",");
            if (entityIdStr != null)
                data.append("\"").append(entityIdStr).append("\":\"").append(entityId).append("\",");
            data.append("\"trace\":\"").append(ctx.getId()).append("\"}");

            resp.setBody(ErrorResponse.compose(errCode, data.toString()));
            return null;
        }
    }

    public static JSONArray parseDataToJSONArray(DataObject obj, HttpResponse resp, String errCode, Context ctx) {
        if (obj==null || obj.getString()==null || obj.getString().length()==0) {
            resp.setBody(ErrorResponse.compose(errCode, "{\"exception\":\"content is empty\"}"));
            return null;
        }

        try {
            return JSON.parseArray(obj.getString());
        } catch (Exception ex) {
            ServiceLogger.error(_position, "Failed to parse content", "errCode="+errCode+", traceId="+ctx.getId(), ex);

            StringBuilder data = new StringBuilder();
            String msg = ex.getMessage();
            int pos = msg.indexOf('\n');
            if (pos >= 0) msg = msg.substring(0,pos);
            data.append("{\"exception\":\"").append(ex.getClass().getName()).append(": ").append(msg).append("\",");
            data.append("\"trace\":\"").append(ctx.getId()).append("\"}");

            resp.setBody(ErrorResponse.compose(errCode, data.toString()));
            return null;
        }
    }

    public static boolean checkExistence(JSONObject obj, String[] fields, HttpResponse resp) {
        boolean result = true;

        for (String field : fields) {
            if (! obj.containsKey(field)) {
                result = false;
                break;
            }
        }

        if (! result) {
            StringBuilder data = new StringBuilder();
            data.append("{\"exception\":\"missing fields: ").append(Arrays.toString(fields)).append("\"}");

            resp.setBody(ErrorResponse.compose("BAD_REQ_BODY", data.toString()));
        }

        return result;
    }

    public static boolean checkParamExistence(HttpRequest req, String[] fields, HttpResponse resp) {
        boolean result = true;

        for (String field : fields) {
            String v = req.getParameter(field);
            if (v==null || v.length()==0) {
                result = false;
                break;
            }
        }

        if (! result) {
            StringBuilder data = new StringBuilder();
            data.append("{\"exception\":\"missing parameters: ").append(Arrays.toString(fields)).append("\"}");

            resp.setBody(ErrorResponse.compose("BAD_REQ_PARAM", data.toString()));
        }

        return result;
    }

    public static boolean checkImmutable(JSONObject obj, String[] fields, HttpResponse resp) {
        boolean result = true;

        for (String field : fields) {
            if (obj.containsKey(field)) {
                result = false;
                break;
            }
        }

        if (! result) {
            StringBuilder data = new StringBuilder();
            data.append("{\"exception\":\"fields are immutable: ").append(Arrays.toString(fields)).append("\"}");

            resp.setBody(ErrorResponse.compose("BAD_REQ_BODY", data.toString()));
        }

        return result;
    }

    /**
     * Partial merge:
     * 1 - If there are fields not in wholeFields or partialFields, error will be responded.
     * 2 - For fields in wholeFields:
     *     2.1 The value of the field of "from" object will replace the field value of "to" object as a whole.
     *     2.2 If the value of the field of "from" object is null, the field of "to" object will be removed.
     * 3 - For fields in partialFields:
     *     3.1 If the value of the field of "from" object is null, the field of "to" object will be copied.
     *     3.2 If the value of the field of "from" object is not an Object, the field of "to" object will be copied.
     *     3.3 If the value of the field (name it "a") of "from" object is an Object (name it "a-object"),
     *     all fields of "a-object" object will be merged, as wholeFields, into the "a" field iof "to" object.
     * 4 - Both "from" and "to" objects will not be changed, but their parts might be copied (passed by reference) to the result object.
     * 5 - If a field is in both wholeFields and partialFields, it will be treated partially.
     * 6 - Fields of "to" object which do not appear in wholeFields or partialFields will be copied.
     */
    public static JSONObject mergeJSONObjects(JSONObject from, JSONObject to, String[] wholeFields, String[] partialFields, HttpResponse resp) {
        Set<String> wholeSet = new HashSet<>();
        Set<String> partialSet = new HashSet<>();
        List<String> unknownFields = new ArrayList<>();
        JSONObject result = new JSONObject();
        Set<String> fromFields = from.keySet();

        if (wholeFields != null) wholeSet.addAll(Arrays.asList(wholeFields));
        else wholeSet.addAll(fromFields);
        if (partialFields != null) partialSet.addAll(Arrays.asList(partialFields));
        if (to == null) to = new JSONObject();

        for (String field : fromFields) {
            if (partialSet.contains(field)) {
                Object vFrom = from.get(field);
                Object vTo = to.get(field);

                if (vFrom!=null && vFrom instanceof JSONObject) {
                    if (vTo!=null && vTo instanceof JSONObject) {
                        JSONObject vMerged = mergeJSONObjects((JSONObject) vFrom, (JSONObject) vTo, null, null, null);
                        result.put(field, vMerged);
                    }
                    else result.put(field, vFrom);
                }
                else {
                    if (vTo != null) result.put(field, vTo);
                }
            }
            else if (wholeSet.contains(field)) {
                Object v = from.get(field);
                if (v != null) result.put(field, v);
            }
            else
                unknownFields.add(field);
        }

        // Copy the untouched fields from "to" to "result"

        Set<String> toFields = new HashSet<>();
        toFields.addAll(to.keySet());
        toFields.removeAll(fromFields);
        for (String field : toFields) result.put(field, to.get(field));

        if (resp!=null && unknownFields.size()>0) {
            StringBuilder data = new StringBuilder();
            data.append("{\"exception\":\"unknown fields: ")
                    .append(Arrays.toString(unknownFields.toArray(new String[]{})))
                    .append("\"}");

            resp.setBody(ErrorResponse.compose("BAD_REQ_BODY", data.toString()));
            result = null;
        }

        return result;
    }

    public static int findItemInJSONArray(JSONArray arr, String itemKey, String itemValue, String data, HttpResponse resp) {
        int pos = -1;

        if (arr!=null && arr.size()>0) {
            for (int i = 0; i < arr.size(); i++) {
                JSONObject item = arr.getJSONObject(i);
                if (itemValue.equals(item.getString(itemKey))) {
                    pos = i;
                    break;
                }
            }
        }

        if (pos < 0) resp.setBody(ErrorResponse.compose("NOT_FOUND", data));

        return pos;
    }

    public static void clearFiles(HttpRequest req) {
        if (req.hasFiles()) {
            Set<String> files = req.getFileNames();
            for (String file : files) {
                try (InputStream in = req.getFileContent(file)) {
                    // Do nothing, just to close the stream
                } catch (Exception ex) {}
            }
        }
    }

    public static JSONObject getParamsFromBodyOrParams(HttpRequest req, String[] fields, HttpResponse resp, Context ctx) {
        JSONObject result = new JSONObject();

        DataObject bodyObj = req.getBody();
        JSONObject body = bodyObj==null ? null : bodyObj.getJSONObject();

        for (String field : fields) {
            String v = null;
            if (body != null) v = body.getString(field);
            if (v==null || v.length()==0) v = req.getParameter(field);
            if (v!=null && v.length()>0) result.put(field, v);
        }

        if (result.size() == 0) {
            StringBuilder data = new StringBuilder();
            data.append("{\"exception\":\"missing parameters: ").append(Arrays.toString(fields)).append("\"}");

            resp.setBody(ErrorResponse.compose("BAD_REQ_PARAM", data.toString()));
            return null;
        }
        else
            return result;
    }

    private static final Pattern patternId = Pattern.compile("[0-9a-zA-Z_]*");
    private static final Pattern patternModule = Pattern.compile("[-0-9a-zA-Z_\\.]*");
    private static final Pattern patternTime = Pattern.compile("[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}");

    public static boolean checkIDPattern(String id, String idField, HttpResponse resp) {
        return _checkStringPattern(patternId, id, 50, idField, "BAD_ID", resp);
    }

    public static boolean checkModulePattern(String str, String strField, HttpResponse resp) {
        return _checkStringPattern(patternModule, str, 50, strField, "BAD_MODULE", resp);
    }

    private static boolean _checkStringPattern(Pattern pattern, String str, int maxLength, String strField, String errCode, HttpResponse resp) {
        if (str==null || str.length()==0) return true;

        boolean result = true;
        if (str.length() > maxLength) result = false;
        else result = pattern.matcher(str).matches();

        if (! result)
            resp.setBody(ErrorResponse.compose(errCode, "{\"field\":\""+strField+"\"}"));

        return result;
    }

    public static boolean checkTimeString(String timeField, String time, HttpResponse resp) {
        if (time==null || time.length()==0) return true;

        boolean result = patternTime.matcher(time).matches();

        if (! result)
            resp.setBody(ErrorResponse.compose("BAD_REQ_TIME", "{\"field\":\""+timeField+"\"}"));

        return result;
    }

    public static boolean checkNotKeywords(String str, String[] keywords, String strField, HttpResponse resp) {
        if (str==null || str.length()==0 || keywords==null || keywords.length==0) return true;

        boolean result = true;
        for (String keyword : keywords) {
            if (str.equalsIgnoreCase(keyword)) {
                result = false;
                break;
            }
        }

        if (! result)
            resp.setBody(ErrorResponse.compose("BAD_REQ_PARAM", "{\""+strField+"\":\""+str+"\"}"));

        return result;
    }

    public static boolean checkAuthorizationResult(String[] authResults, HttpResponse resp) {
        if (authResults==null || authResults.length==0) return true;

        String authResult = authResults[0];
        if (authResult==null || authResult.length()==0) return true;

        resp.setBody(
                authResults.length==1 ?
                        ErrorResponse.compose(authResult) :
                        ErrorResponse.compose(authResult, "{\"exception\":\""+authResults[1]+"\"}")
        );

        return false;
    }

}
