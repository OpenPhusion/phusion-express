package cloud.phusion.express.integration;

import cloud.phusion.DataObject;
import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONArray;
import com.alibaba.fastjson2.JSONObject;
import org.codehaus.janino.ExpressionEvaluator;

import java.util.ArrayList;
import java.util.Set;

/**
 * Expression Evaluator
 *
 * Format:
 * {
 *   expression: String, // Java expression which must be evaluated into a boolean result
 *   vars: {
 *       "<var>": { // The variable name
 *           type: "String | Boolean | Integer | Long | Float | Double",
 *           value: Any, // The constant value of the variable
 *           fromMessage: "<field>[.<field>]", // The value of the variable comes from the message
 *           fromConfig: "<field>[.<field>]" // The value of the variable comes from the integration configuration
 *       }
 *   }
 * }
 *
 * Caution: "floatA == floatB" may give a wrong result, use "floatA.equals(floatB)" or "floatA == 1.0" instead.
 */
public class ConditionEvaluator {

    private ExpressionEvaluator evaluator;
    private JSONObject vars;

    /**
     * Sometimes it is better to load condition as JSONObject, so String and JSONObject are both acceptable
     */
    public ConditionEvaluator(Object condition) throws Exception {
        super();

        if (condition == null) {
            evaluator = null;
            vars = null;
        }
        else {
            JSONObject objCond = (condition instanceof String) ? JSON.parseObject((String) condition) : (JSONObject) condition;
            vars = objCond.getJSONObject("vars");

            evaluator = new org.codehaus.janino.ExpressionEvaluator();

            if (vars!=null && vars.size()>0) _setExpressionParameters();
            else vars = null;

            evaluator.setExpressionType(Boolean.class);
            evaluator.cook(objCond.getString("expression"));
        }
    }

    private void _setExpressionParameters() {
        ArrayList<String> params = new ArrayList<>();
        ArrayList<Class> types = new ArrayList<>();
        Set<String> varNames = vars.keySet();

        for (String varName : varNames) {
            params.add(varName);

            switch (vars.getJSONObject(varName).getString("type")) {
                case "String": types.add(String.class); break;
                case "Boolean": types.add(Boolean.class); break;
                case "Integer": types.add(Integer.class); break;
                case "Long": types.add(Long.class); break;
                case "Float": types.add(Float.class); break;
                case "Double": types.add(Double.class); break;
            }
        }

        String[] arrParams = params.toArray(new String[]{});
        Class[] arrTypes = types.toArray(new Class[]{});

        evaluator.setParameters(arrParams, arrTypes);
    }

    public boolean evaluate(DataObject msg, DataObject config) throws Exception {
        if (evaluator == null) return true;

        JSONObject objMsg = msg==null ? null : msg.getJSONObject();
        JSONObject objConfig = config==null ?  null : config.getJSONObject();
        Object result = null;

        if (vars != null) {
            ArrayList<Object> params = new ArrayList<>();
            Set<String> varNames = vars.keySet();

            for (String varName : varNames) {
                params.add( _processExpressionParam(varName, objMsg, objConfig) );
            }

            Object[] arrParams = params.toArray(new Object[]{});
            result = evaluator.evaluate(arrParams);
        }
        else
            result = evaluator.evaluate();

        return result==null ? false : (Boolean) result;
    }

    private Object _processExpressionParam(String varName, JSONObject msg, JSONObject config) {
        JSONObject var = vars.getJSONObject(varName);
        String type = var.getString("type");

        String msgField = var.containsKey("fromMessage") ? var.getString("fromMessage") : null;
        String cfgField = var.containsKey("fromConfig") ? var.getString("fromConfig") : null;
        if (msgField!=null && msgField.length()==0) msgField = null;
        if (cfgField!=null && cfgField.length()==0) cfgField = null;

        if (msgField!=null || cfgField!=null) {
            JSONObject objValue = msgField!=null ? msg : config;
            String field = msgField!=null ? msgField : cfgField;

            if (objValue == null) return null;
            else {
                int ipos = field.indexOf("."); // Fields in the path are connected with "."

                while (ipos >= 0) {
                    String firstLayerField = field.substring(0,ipos);
                    Object firstLayer = objValue.get(firstLayerField);

                    if (firstLayer == null) return null;
                    else if (firstLayer instanceof JSONObject) objValue = (JSONObject) firstLayer;
                    else if (firstLayer instanceof JSONArray) {
                        // If it is an array, check the first element only
                        if (((JSONArray)firstLayer).size() == 0) return null;
                        else objValue = ((JSONArray)firstLayer).getJSONObject(0);
                    }

                    field = field.substring(ipos+1);
                    ipos = field.indexOf("_");
                }

                return _retrieveValue(objValue, field, type);
            }
        }
        else { // from constant value
            return _retrieveValue(var, "value", type);
        }
    }

    private Object _retrieveValue(JSONObject obj, String field, String type) {
        Object result = null;

        if (obj != null) {
            switch (type) {
                case "String":
                    result = obj.getString(field);
                    break;
                case "Integer":
                    result = obj.getInteger(field);
                    break;
                case "Long":
                    result = obj.getLong(field);
                    break;
                case "Float":
                    result = obj.getFloat(field);
                    break;
                case "Double":
                    result = obj.getDouble(field);
                    break;
                case "Boolean":
                    result = obj.getBoolean(field);
                    if (result == null) result = new Boolean(false);
                    break;
            }
        }

        return result;
    }

}
