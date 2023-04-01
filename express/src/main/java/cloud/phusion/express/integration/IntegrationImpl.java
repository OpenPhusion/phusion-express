package cloud.phusion.express.integration;

import cloud.phusion.*;
import cloud.phusion.application.*;
import cloud.phusion.express.ExpressContext;
import cloud.phusion.express.util.FullTextEncoder;
import cloud.phusion.express.util.TimeMarker;
import cloud.phusion.integration.*;
import cloud.phusion.storage.DBStorage;
import cloud.phusion.storage.Record;
import cloud.phusion.dev.IntegrationMocker;
import com.alibaba.fastjson2.JSONObject;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * It is thread-safe.
 *
 * The thread to execute it:
 * - when start from an HTTP request, run in the Tomcat service thread.
 * - when start from the scheduler, run in the Quartz service thread.
 * - otherwise, run in the main thread (maybe).
 *
 * Transaction logging:
 * - for each step, only logged the incoming status of the transaction.
 * - the dull starting steps are ignored.
 * - the probing executions are ignored
 */
public class IntegrationImpl implements Integration {
    private static final String _position = IntegrationImpl.class.getName();

    private String itId = null; // Integration ID
    private String clientId = null;
    private Step firstStep = null;
    private ConditionEvaluator evaluator = null;
    private IntegrationDefinition def = null;
    private DataObject itConfig = null; // Integration configuration in JSON string
    private boolean stopped = true; // Whether the integration is stopped
    private boolean endpointRegistered = false; // Whether bound to the application endpoints according to StepEndpoint defined in the workflow
    private Map<String,String> startedTasks = new ConcurrentHashMap<>();

    private static String trxlogTarget = null;
    private static boolean trxlogEncodeMsg = false;

    private static IntegrationMocker itMocker;

    public static void initSystemSettings(String target, String encodeMsg, Context ctx) throws Exception {
        trxlogTarget = (target!=null && target.length()==0) ? null : target;

        if (trxlogTarget != null) {
            trxlogEncodeMsg = "true".equals(encodeMsg);
        }
    }

    public static void prepareDBTables(String targetNamespace, Context ctx) throws Exception {
            DBStorage storage = ctx.getEngine().getDBStorageForApplication(targetNamespace);

            storage.prepareTable(
                    EngineFactory.TRXLog_Table_Transaction,
                    "{" +
                            "    \"fields\": {" +
                            "        \"id\": \"Long\"," +
                            "        \"integrationId\": \"String[50]\"," +
                            "        \"engineId\": \"String[10]\"," +
                            "        \"failed\": \"Boolean\"," +
                            "        \"finished\": \"Boolean\"," +
                            "        \"startTime\": \"String[20]\"," +
                            "        \"duration\": \"Double\"" +
                            "    }," +
                            "    \"indexes\": [" +
                            "        {\"field\": \"id\", \"primary\": true}," +
                            "        {\"field\": \"integrationId\"}," +
                            "        {\"field\": \"startTime\"}" +
                            "    ]" +
                            "}",
                    ctx
            );

            storage.prepareTable(
                    EngineFactory.TRXLog_Table_TransactionStep,
                    "{" +
                            "    \"fields\": {" +
                            "        \"id\": \"Long\"," +
                            "        \"transactionId\": \"Long\"," +
                            "        \"step\": \"String[20]\"," +
                            "        \"fromStep\": \"String[20]\"," +
                            "        \"duration\": \"Double\"" +
                            "    }," +
                            "    \"indexes\": [" +
                            "        {\"field\": \"id\", \"primary\": true}," +
                            "        {\"field\": \"transactionId\"}" +
                            "    ]" +
                            "}",
                    ctx
            );

            storage.prepareTable(
                    EngineFactory.TRXLog_Table_TransactionStepInfo,
                    "{" +
                            "    \"fields\": {" +
                            "        \"stepId\": \"Long\"," +
                            "        \"config\": \"String[5000]\"," +
                            "        \"msg\": \"String[5000]\"," +
                            "        \"properties\": \"String[5000]\"" +
                            "    }," +
                            "    \"indexes\": [" +
                            "        {\"field\": \"stepId\", \"primary\": true}," +
                            "        {\"field\": \"msg\", \"fulltext\": true}" +
                            "    ]" +
                            "}",
                    ctx
            );
    }

    public IntegrationImpl(IntegrationDefinition idef) throws Exception {
        super();

        if (idef == null) return;

        this.def = idef;
        this.firstStep = idef.getFirstStep();
        this.endpointRegistered = false;
        this.evaluator = new ConditionEvaluator(idef.getStartCondition());
    }

    @Override
    public void setId(String id) {
        this.itId = id;
    }

    @Override
    public String getId() {
        return itId;
    }

    @Override
    public void setClientId(String id) {
        this.clientId = id;
    }

    @Override
    public String getClientId() {
        return clientId;
    }

    @Override
    public void init(DataObject config, Context ctx) throws Exception {
        this.itConfig = config;
        this.stopped = true;

        ctx.logInfo(_position, "Integration initialized");
    }

    @Override
    public boolean canStart(DataObject msg) throws Exception {
        return evaluator.evaluate(msg, itConfig);
    }

    @Override
    public void start(Context ctx) throws Exception {
        if (! stopped) return;

        this.stopped = false;
        ctx.setContextInfo("integrationId", itId);

        _registerEndpoints(def, ctx);

        // Inbound endpoints are not considered here, and they will be registered to the HTTP server by _registerEndpoints
        if (!(firstStep instanceof StepEndpoint) || ((StepEndpoint) firstStep).getDirection()==Direction.Out) {
            String taskId = "I" + itId;
            IntegrationStarter starter = new IntegrationStarter(this);

            if (def.isCronScheduled()) {
                ctx.getEngine().scheduleTask(
                        taskId,
                        starter,
                        def.getCron(),
                        def.isClustered(),
                        ctx
                );
                startedTasks.put(taskId,"");
            } else if (def.isPeriodic()) {
                ctx.getEngine().scheduleTask(
                        taskId,
                        starter,
                        def.getStartTime(),
                        def.getIntervalInSeconds(),
                        def.getRepeatCount(),
                        def.isClustered(),
                        ctx
                );
                startedTasks.put(taskId,"");
            } else {
                // Not scheduled, run once immediately

                Context taskCtx = new ExpressContext(ctx.getEngine());
                taskCtx.setContextInfo("integrationId", itId);

                starter.run(taskId, taskCtx);
            }
        }

        ctx.logInfo(_position, "Integration started");
        ctx.removeContextInfo("integrationId");
    }

    @Override
    public void stop(Context ctx) throws Exception {
        if (stopped) return;

        this.stopped = true;
        ctx.setContextInfo("integrationId", itId);

        _unregisterEndpoints(def, ctx);

        // Stop all scheduled tasks

        Set<String> tasks = startedTasks.keySet();
        Engine engine = ctx.getEngine();

        for (String task : tasks) {
            engine.removeScheduledTask(task, ctx);
        }

        startedTasks.clear();

        ctx.logInfo(_position, "Integration stopped");
        ctx.removeContextInfo("integrationId");
    }

    @Override
    public ExecStatus getStatus() {
        return stopped ? ExecStatus.Stopped : ExecStatus.Running;
    }

    private void _registerEndpoints(IntegrationDefinition idef, Context ctx) throws Exception {
        if (! endpointRegistered) {
            _doRegisterEndpoints(idef, ctx, true);
            endpointRegistered = true;
        }
    }

    private void _unregisterEndpoints(IntegrationDefinition idef, Context ctx) throws Exception {
        if (endpointRegistered) {
            _doRegisterEndpoints(idef, ctx, false);
            endpointRegistered = false;
        }
    }

    /**
     * For all endpoints defined in the workflow, bind the integration to them
     * @param registering true: register; false: unregister
     */
    private void _doRegisterEndpoints(IntegrationDefinition idef, Context ctx, boolean registering) throws Exception {
        Step[] steps = idef.getSteps();
        Engine engine = ctx.getEngine();

        for (int i=0; i<steps.length; i++) {
            if (!(steps[i] instanceof StepEndpoint)) continue;

            StepEndpoint step = (StepEndpoint) steps[i];
            String appId = step.getApplication();

            // If the application of the endpoint is not running, do not bind
            if (engine.getApplicationStatus(appId) != ExecStatus.Running) continue;

            Application app = engine.getApplication(appId);

            if (registering) app.addEndpointForIntegration(step.getEndpoint(), itId, step.getConnectionId(), step.getConfig());
            else app.removeEndpointForIntegration(step.getEndpoint(), itId);
        }
    }

    @Override
    public void destroy(Context ctx) throws Exception {
        if (! stopped) throw new PhusionException("IT_RUN", "Failed to destroy integration", ctx);

        ctx.logInfo(_position, "Integration destroyed");
    }

    @Override
    public DataObject execute(Context ctx) throws Exception {
        return execute(null, ctx);
    }

    @Override
    public DataObject execute(DataObject msg, Context ctx) throws Exception {
        if (itMocker != null) return itMocker.executeIntegration(itId, msg);

        DataObject result = null;

        if (canStart(msg)) {
            Transaction trx = createInstance(msg, ctx);
            result = runInstance(trx);
        }
        return result;
    }

    @Override
    public Transaction createInstance(Context ctx) throws Exception {
        return createInstance(null, null, null, false, null, ctx);
    }

    @Override
    public Transaction createInstance(DataObject msg, Context ctx) throws Exception {
        return createInstance(msg, null, null, false, null, ctx);
    }

    @Override
    public Transaction createInstance(DataObject msg, String step, String previousStep, boolean failed, Map<String, Object> properties, Context ctx) throws Exception {
        String id = "" + ctx.getEngine().generateUniqueId(ctx);

        Transaction trx = null;

        if (step == null) {
            // It is creating a normal instance for execution

            if (msg != null) {
                // If there's a message, it indicates the first step has been executed already.
                // In this situation, find the next step to move to. If there are multiple next steps, choose the first one.

                Step[] steps = def.getNextSteps(firstStep.getId());
                String next = (steps == null || steps.length == 0) ? null : steps[0].getId();

                trx = new Transaction(itId, id, next, firstStep.getId(), ctx);
                trx.setMessage(msg);
            } else {
                trx = new Transaction(itId, id, firstStep.getId(), ctx);
            }

            trx.setIntegrationConfig(itConfig);
        }
        else {
            // It is creating a test instance

            trx = new Transaction(itId, id, step, ctx);
            trx.updateAll(step, previousStep, failed, msg, itConfig, properties);
        }

        trx.setClientId(clientId);
        ctx.setContextInfo("transactionId", id);
        ctx.setContextInfo("integrationId", itId);

        return trx;
    }

    @Override
    public void updateConfig(DataObject config, Context ctx) throws Exception {
        ctx.setContextInfo("integrationId", itId);

//        if (! stopped) throw new PhusionException("IT_RUN", "Failed to update integration config", ctx);

        ctx.logInfo(_position, "Integration config is updated", "oldConfig="+(itConfig==null?"":itConfig.getString())+", config="+(config==null?"":config.getString()));

        itConfig = config;

        ctx.removeContextInfo("integrationId");
    }

    @Override
    public void updateStepMsg(String stepId, DataObject msg, Context ctx) throws Exception {
        ctx.setContextInfo("integrationId", itId);

//        if (! stopped) throw new PhusionException("IT_RUN", "Failed to update step message", ctx);

        Step step = def.getStepById(stepId);
        if (!(step instanceof StepDirect))
            throw new PhusionException("IT_OP", "Can not update step message other than StepDirect", ctx);

        DataObject oldMsg = ((StepDirect) step).getMessage();
        ctx.logInfo(_position, "Step message is updated", String.format("step=%s, oldMsg=%s, msg=%s",
                stepId, oldMsg==null?"":oldMsg.getString(1024), msg==null?"":msg.getString(1024)));

        ((StepDirect)step).setMessage(msg);

        ctx.removeContextInfo("integrationId");
    }

    @Override
    public DataObject runInstance(Transaction trx) throws Exception {
        return _runInstance(trx, true, false);
    }

    @Override
    public void probe(Transaction trx) throws Exception {
        probe(trx, false);
    }

    @Override
    public void probe(Transaction trx, boolean moveOn) throws Exception {
        _registerEndpoints(def, trx.getContext());
        _runInstance(trx, moveOn, true);
//        _unregisterEndpoints(def, trx.getContext());
    }

    /**
     * Run the instance (transaction) of the integration, and return the output message.
     *
     * @param moveOn false: execute just one step; true: execute all steps after this step
     * @param probing whether it is testing the workflow. When the integration is stopped, it still can be tested
     */
    private DataObject _runInstance(Transaction trx, boolean moveOn, boolean probing) throws Exception {
        Context ctx = trx.getContext();
        if (!probing && stopped)
            throw new PhusionException("IT_STOP", "Failed to run integration instance", ctx);

        ctx.logInfo(_position, "Running integration instance", String.format("config=%s, moveOn=%b, probing=%b",
                trx.getIntegrationConfig()==null?"":trx.getIntegrationConfig().getString(), moveOn, probing));

        // Print the first message (input message for the whole integration), but ignore the probing message
        if (!probing) logTransaction(trx, 0);

        TimeMarker m = new TimeMarker();

        StepExecutor exe = new StepExecutor(def, probing);
        if (moveOn) {
            while (!trx.isFinished()) exe.execute(trx);
        }
        else exe.execute(trx);

        double ms = m.mark();
        ctx.logInfo(_position, "Integration instance runned", String.format("time=%.1fms", ms));

        return trx.getMessage();
    }

    public static void logTransaction(Transaction trx, long stepStartTime) throws Exception {
        Context ctx = trx.getContext();
        double duration = stepStartTime==0 ? 0.0 : ((System.nanoTime()-stepStartTime)/100000/10.0);

        if (trxlogTarget == null) {
            ctx.logInfo(_position, "Step", String.format("step=%s, fromStep=%s, msg=%s, properties=%s, failed=%b, finished=%b, time=%.1fms",
                    trx.getCurrentStep(), trx.getPreviousStep(), trx.getMessage() == null ? "" : trx.getMessage().getString(500),
                    trx.getProperties(), trx.isFailed(), trx.isFinished(), trx.getTimeInMilliseconds()));
        }
        else {
            Engine engine = ctx.getEngine();
            DBStorage storage = engine.getDBStorageForApplication(trxlogTarget);
            Record record;

            // If stepStartTime = 0, it is the first step to be logged.

            // Store transaction

            if (stepStartTime == 0) {
                // Store transaction information for the first time.

                record = new Record();
                record.setValue("id", trx.getId());
                record.setValue("integrationId", trx.getIntegrationId());
                record.setValue("engineId", engine.getId());
                record.setValue("failed", false);
                record.setValue("finished", false);
                record.setValue("startTime", _getDatetimeString());
                record.setValue("duration", 0.0);

                storage.insertRecord(EngineFactory.TRXLog_Table_Transaction, record, ctx);
            } else if (trx.isFinished()) {
                // Store transaction information for the last time.

                record = new Record();
                record.setValue("failed", trx.isFailed());
                record.setValue("finished", trx.isFinished());
                record.setValue("duration", trx.getTimeInMilliseconds());

                storage.updateRecordById(EngineFactory.TRXLog_Table_Transaction, record, "id", trx.getId(), ctx);
            }

            // Store transaction step

            long stepId = engine.generateUniqueId(ctx);
            String step;
            record = new Record();
            record.setValue("id", stepId);
            record.setValue("transactionId", trx.getId());
            step = trx.getCurrentStep();
            record.setValue("step", step==null ? "" : step);
            step = trx.getPreviousStep();
            record.setValue("fromStep", step==null ? "" : step);
            record.setValue("duration", duration);

            storage.insertRecord(EngineFactory.TRXLog_Table_TransactionStep, record, ctx);

            // Store transaction step info

            DataObject obj;
            record = new Record();
            record.setValue("stepId", stepId);
            if (stepStartTime == 0) {
                // Store integration config only at the first step.
                obj = trx.getIntegrationConfig();
                if (obj != null) record.setValue("config", obj.getString());
            }
            obj = trx.getMessage();
            if (obj != null) {
                String str = obj.getString();
                if (str!=null && str.length()>0) {
                    if (trxlogEncodeMsg) str = FullTextEncoder.encode(str);
                    record.setValue("msg", str);
                }
            }

            Map<String,Object> props = trx.getProperties();
            if (props!=null && props.size()>0) {
                JSONObject objProps = new JSONObject();
                objProps.putAll(props);
                record.setValue("properties", objProps.toJSONString());
            }

            storage.insertRecord(EngineFactory.TRXLog_Table_TransactionStepInfo, record, ctx);
        }

    }

    private static String _getDatetimeString() {
        SimpleDateFormat df = new SimpleDateFormat(EngineFactory.DATETIME_FORMAT);
        return df.format(new Date());
    }

    public static void setIntegrationMocker(IntegrationMocker itm) {
        itMocker = itm;
    }

}
