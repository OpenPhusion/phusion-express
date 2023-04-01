package cloud.phusion.express.controller;

import cloud.phusion.Context;
import cloud.phusion.DataObject;
import cloud.phusion.PhusionException;
import cloud.phusion.express.service.AuthorizationService;
import cloud.phusion.express.service.TransactionService;
import cloud.phusion.express.util.CommonCode;
import cloud.phusion.express.util.CommonHandler;
import cloud.phusion.express.util.ErrorResponse;
import cloud.phusion.protocol.http.HttpMethod;
import cloud.phusion.protocol.http.HttpRequest;
import cloud.phusion.protocol.http.HttpResponse;
import com.alibaba.fastjson2.JSONObject;

import java.util.*;

public class TransactionController {
    private static final long maxSearchTimeDiff = 3; // 3 days
    private static final long maxMinuteStatsTimeDiff = 1; // 1 days

    private static final String thisCategory = "transaction";

    private static final Set<String> predefinedKeywords = new HashSet<>(
            Arrays.asList("step")
    );

    public static void dispatch(JSONObject currentUser, HttpRequest req, HttpResponse resp, Context ctx) throws Exception {
        String item = req.getParameter("_item");
        String action = req.getMethod().toString();

        if (item == null) action += " null";
        else if (predefinedKeywords.contains(item)) action += " "+item;
        else action += " id";

        switch (action) {
            case "GET null":
                String statsOnlyStr = req.getParameter("statsOnly");
                boolean statsOnly = "true".equals(statsOnlyStr);
                String groupBy = req.getParameter("groupBy");
                if (groupBy!=null && groupBy.length()==0) groupBy = null;

                if (statsOnly || groupBy!=null) _getTransactionStats(currentUser, groupBy, req, resp, ctx);
                else _listTransactions(currentUser, req, resp, ctx);
                break;
            case "GET id": _getTransaction(currentUser, item, req, resp, ctx); break;
            case "GET step": _getTransactionStepStats(currentUser, req, resp, ctx); break;
            default: resp.setBody(ErrorResponse.compose("BAD_REQ_URL"));
        }
    }

    private static void _listTransactions(JSONObject currentUser, HttpRequest req, HttpResponse resp, Context ctx) throws Exception {
        String ids = req.getParameter("ids");
        String fields = req.getParameter("fields");
        if (fields!=null && fields.length()==0) fields = null;

        DataObject result;

        try {
            if (ids == null || ids.length() == 0) {
                List<String> whereFields = new ArrayList<>();
                List<Object> whereParams = new ArrayList<>();
                if (!_getWhereParams(currentUser, req, whereFields, whereParams, null, resp, ctx)) return;

                String fromStr = req.getParameter("from");
                String lengthStr = req.getParameter("length");
                long from = (fromStr == null || fromStr.length() == 0) ? 0 : Long.parseLong(fromStr);
                long length = (lengthStr == null || lengthStr.length() == 0) ? 100 : Long.parseLong(lengthStr);

                result = TransactionService.listTransactions(fields, whereFields, whereParams, from, length, ctx);
            } else {
                if (! CommonHandler.checkAuthorizationResult(
                        AuthorizationService.checkPrivilege(currentUser, HttpMethod.GET, thisCategory, null, null, ctx),
                        resp)) return;

                result = TransactionService.listTransactionsById(ids, fields, ctx);
            }
        } catch (PhusionException ex) {
            _handleTrxException(ex, resp);
            return;
        }

        if (result == null) resp.setBody(new DataObject("{}"));
        else resp.setBody(new DataObject("{\"result\":"+result.getString()+"}"));
    }

    private static void _getTransaction(JSONObject currentUser, String id, HttpRequest req, HttpResponse resp, Context ctx) throws Exception {
        DataObject trxObj;

        try {
            trxObj = TransactionService.fetchTransaction(id, ctx);
        } catch (PhusionException ex) {
            _handleTrxException(ex, resp);
            return;
        }

        if (trxObj == null) {
            resp.setBody(ErrorResponse.compose("NOT_FOUND", "{\"transactionId\":\""+id+"\"}"));
            return;
        }

        JSONObject trx = trxObj.getJSONObject();
        Map<String,Object> params = new HashMap<>();
        params.put("integrationId", trx.getString("integrationId"));
        if (! CommonHandler.checkAuthorizationResult(
                AuthorizationService.checkPrivilege(currentUser, HttpMethod.GET, thisCategory, "id", params, ctx),
                resp)) return;

        resp.setBody(new DataObject("{\"result\":"+trxObj.getString()+"}"));
    }

    private static void _getTransactionStats(JSONObject currentUser, String groupBy, HttpRequest req, HttpResponse resp, Context ctx) throws Exception {
        DataObject result;
        List<String> whereFields = new ArrayList<>();
        List<Object> whereParams = new ArrayList<>();

        if (! _getWhereParams(currentUser, req, whereFields, whereParams, groupBy, resp, ctx)) return;

        try {
            if (groupBy == null)
                result = TransactionService.getTransactionStats(whereFields, whereParams, ctx);
            else
                result = TransactionService.getTransactionGroupStats(whereFields, whereParams, groupBy, ctx);
        } catch (PhusionException ex) {
            _handleTrxException(ex, resp);
            return;
        }

        if (result == null) {
            if (groupBy == null)
                resp.setBody(new DataObject("{\"result\":{\"count\":0}}"));
            else
                resp.setBody(new DataObject("{}"));
        }
        else resp.setBody(new DataObject("{\"result\":"+result.getString()+"}"));
    }

    private static void _getTransactionStepStats(JSONObject currentUser, HttpRequest req, HttpResponse resp, Context ctx) throws Exception {
        if (! CommonHandler.checkParamExistence(req, new String[]{"integrationId"}, resp)) return;

        String id = req.getParameter("integrationId");
        String startTime = req.getParameter("startTime");
        String endTime = req.getParameter("endTime");

        Map<String,Object> params = new HashMap<>();
        params.put("integrationId", id);
        if (! CommonHandler.checkAuthorizationResult(
                AuthorizationService.checkPrivilege(currentUser, HttpMethod.GET, thisCategory, "step", params, ctx),
                resp)) return;

        DataObject result = null;

        try {
            result = TransactionService.getTransactionStepStats(id, startTime, endTime, ctx);
        } catch (PhusionException ex) {
            _handleTrxException(ex, resp);
            return;
        }

        resp.setBody(new DataObject("{\"result\":"+result.getString()+"}"));
    }

    //----------

    private static boolean _getWhereParams(JSONObject currentUser, HttpRequest req, List<String> whereFields, List<Object> whereParams, String groupBy, HttpResponse resp, Context ctx) throws Exception {
        String search = req.getParameter("search");
        String integrationId = req.getParameter("integrationId");
        String applicationId = req.getParameter("applicationId");
        String clientId = req.getParameter("clientId");
        String engineId = req.getParameter("engineId");
        String startTime = req.getParameter("startTime");
        String endTime = req.getParameter("endTime");

        if (search!=null && search.length()==0) search = null;
        if (integrationId!=null && integrationId.length()==0) integrationId = null;
        if (applicationId!=null && applicationId.length()==0) applicationId = null;
        if (clientId!=null && clientId.length()==0) clientId = null;
        if (engineId!=null && engineId.length()==0) engineId = null;
        if (startTime!=null && startTime.length()==0) startTime = null;
        if (endTime!=null && endTime.length()==0) endTime = null;

        if (clientId==null && currentUser!=null) {
            clientId = currentUser.getString("clientId");
            if (clientId!=null && clientId.length()==0) clientId = null;
        }

        if (! CommonHandler.checkTimeString("startTime", startTime, resp)) return false;
        if (! CommonHandler.checkTimeString("endTime", endTime, resp)) return false;
        if (! _checkSearchConditions(search, integrationId, startTime, endTime, groupBy, resp)) return false;
        if (! _checkGroupStatsConditions(groupBy, startTime, endTime, resp)) return false;

        Map<String,Object> authParams = new HashMap<>();

        if (search != null) {
            whereFields.add("search");
            whereParams.add(search);
        }
        if (integrationId != null) {
            whereFields.add("integrationId");
            whereParams.add(integrationId);
            authParams.put("integrationId", integrationId);
        }
        if (applicationId != null) {
            whereFields.add("applicationId");
            whereParams.add(applicationId);
            authParams.put("applicationId", applicationId);
        }
        if (clientId != null) {
            whereFields.add("clientId");
            whereParams.add(clientId);
            authParams.put("clientId", clientId);
        }
        if (engineId != null) {
            whereFields.add("engineId");
            whereParams.add(engineId);
        }
        if (startTime != null) {
            whereFields.add("startTime");
            whereParams.add(startTime);
        }
        if (endTime != null) {
            whereFields.add("endTime");
            whereParams.add(endTime);
        }

        return CommonHandler.checkAuthorizationResult(
                AuthorizationService.checkPrivilege(currentUser, HttpMethod.GET, thisCategory, null, authParams, ctx),
                resp);
    }

    private static boolean _checkSearchConditions(String search, String integrationId, String startTime, String endTime, String groupBy, HttpResponse resp) throws Exception {
        if (search == null) return true;

        boolean result = integrationId!=null && startTime!=null && groupBy==null;

        if (result) {
            if (endTime == null) endTime = CommonCode.convertDatetimeString();
            long diff = CommonCode.getTimeDifference(endTime, startTime);
            if (diff > maxSearchTimeDiff * 24 * 60 * 60) result = false;
        }

        if (! result) {
            String data = "{\"exception\":\"Must specify integrationId with search phrases, and give a time range shorter than "+maxSearchTimeDiff+" days. And can not use search with groupBy\"}";
            resp.setBody(ErrorResponse.compose("BAD_REQ_PARAM", data));
        }

        return result;
    }

    private static boolean _checkGroupStatsConditions(String groupBy, String startTime, String endTime, HttpResponse resp) throws Exception {
        if (! "minute".equals(groupBy)) return true;

        boolean result = startTime != null;

        if (result) {
            if (endTime == null) endTime = CommonCode.convertDatetimeString();
            long diff = CommonCode.getTimeDifference(endTime, startTime);
            if (diff > maxMinuteStatsTimeDiff * 24 * 60 * 60) result = false;
        }

        if (! result) {
            String data = "{\"exception\":\"Must give a time range shorter than "+maxMinuteStatsTimeDiff+" days, when grouping by minute\"}";
            resp.setBody(ErrorResponse.compose("BAD_REQ_PARAM", data));
        }

        return result;
    }

    private static void _handleTrxException(PhusionException ex, HttpResponse resp) throws Exception {
        String code = ex.getCode();

        if (code.equals("TRX_LOG_NONE"))
            resp.setBody(ErrorResponse.compose("OP_NONE", "{\"exception\":\"transactions may not be persisted in database\"}"));
        else
            throw ex;
    }

}
