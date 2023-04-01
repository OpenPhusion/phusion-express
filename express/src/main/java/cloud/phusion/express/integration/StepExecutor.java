package cloud.phusion.express.integration;

import cloud.phusion.*;
import cloud.phusion.application.*;
import cloud.phusion.integration.*;
import com.alibaba.fastjson2.JSONArray;

import java.util.ArrayList;

/**
 * ForEach loop:
 * - Currently, StepForEach-StepCollect can not be nested.
 * - Can not go midway into or out of a loop.
 */
public class StepExecutor {
    private static final String _position = StepExecutor.class.getName();

    private IntegrationDefinition idef;
    private boolean probing;

    private ArrayList<DataObject> msgItems; // The items emitted by a ForEach step
    private ArrayList<DataObject> msgResultItems; // The items already processed by a round of the ForEach loop
    private int msgItemPointer; // The current item to be processed. -1 means not in a ForEach loop
    private String currentForEachStep; // The current ForEach step (loop head)

    public StepExecutor(IntegrationDefinition idef, boolean probing) {
        super();

        this.idef = idef;
        this.probing = probing;
        this.msgItemPointer = -1;
        this.currentForEachStep = null;
    }

    public void execute(Transaction trx) throws Exception {
        String currentStep = trx.getCurrentStep();
        Context ctx = trx.getContext();
        assert currentStep != null;
        long startTime = System.nanoTime();

        Step step = idef.getStepById(currentStep);
        boolean failed = false;
        String failMsg = null;

        try {
            switch (step.getClass().getSimpleName()) {
                case "StepDirect": _executeStep((StepDirect)step, trx); break;
                case "StepEndpoint": _executeStep((StepEndpoint)step, trx); break;
                case "StepJava": _executeStep((StepJava)step, trx); break;
                case "StepJavaScript": _executeStep((StepJavaScript)step, trx); break;
                case "StepForEach": _executeStep((StepForEach) step, trx); break;
                case "StepCollect": _executeStep((StepCollect) step, trx); break;
            }
        } catch (Exception ex) {
            failed = true;
            failMsg = ex instanceof PhusionException ?
                        String.format("traceId=%s, step=%s", ((PhusionException) ex).getContextId(), currentStep) :
                        ex.getMessage();

            ctx.logError("IT_EXEC", "Failed to execute the step", "step="+step.getId(), ex);
        }

        if (failed) {
            if (! trx.isFailed()) {
                trx.moveToException(failMsg);
                _clearForEachLoopStates();
            }
            else trx.moveToEnd(); // Avoid dead-loop of exceptions
        }
        else {
            // If the step processor do not move the transaction to the next step, automatically move
            // If multiple next steps are found, choose the first one
            if (currentStep.equals(trx.getCurrentStep())) {
                Step[] steps = idef.getNextSteps(currentStep);

                if (steps == null || steps.length == 0) trx.moveToEnd();
                else trx.moveToStep(steps[0].getId());
            }
        }

        if (! probing) IntegrationImpl.logTransaction(trx, startTime);
    }

    private void _executeStep(StepDirect step, Transaction trx) throws Exception {
        trx.setMessage(step.getMessage());
    }

    private void _executeStep(StepEndpoint step, Transaction trx) throws Exception {
        if (step.getDirection() == Direction.In) return;

        Context ctx = trx.getContext();
        assert ctx != null;

        Engine engine = ctx.getEngine();
        String appId = step.getApplication();
        String connId = step.getConnectionId();
        String itId = trx.getIntegrationId();
        String epId = step.getEndpoint();

        if (engine.getApplicationStatus(appId) != ExecStatus.Running)
            throw new PhusionException("APP_NONE_STOP", "Failed to execute step", String.format("step=%s, applicationId=%s",
                    step.getId(), appId), ctx);

        Application app = engine.getApplication(appId);

        if (app.getConnectionStatus(connId) != ConnectionStatus.Connected)
            throw new PhusionException("APP_NONE_STOP", "Failed to execute step", String.format("step=%s, applicationId=%s, connectionId=%s",
                    step.getId(), appId, connId), ctx);

        // Bind the integration to the endpoint before calling it
        if (! app.hasEndpointForIntegration(epId, itId))
            app.addEndpointForIntegration(epId, itId, connId, step.getConfig());

        DataObject result = app.callOutboundEndpoint(epId, itId, trx.getMessage(), ctx);
        trx.setMessage(result);
    }

    private void _executeStep(StepJava step, Transaction trx) throws Exception {
        Context ctx = trx.getContext();
        assert ctx != null;

        Engine engine = ctx.getEngine();
        String module = step.getModuleId();

        if (engine.doesJavaModuleExist(module)) {
            Processor p = (Processor) engine.createClassInstance(module, step.getProcessorClass(), ctx);
            p.process(trx);
        }
        else
            throw new PhusionException("JAR_NONE", "Failed to execute step", String.format("step=%s, module=%s, className=%s",
                    step.getId(), module, step.getProcessorClass()), ctx);
    }

    private void _executeStep(StepJavaScript step, Transaction trx) throws Exception {
        Context ctx = trx.getContext();
        assert ctx != null;

        Engine engine = ctx.getEngine();
        String script = step.getScriptId();

        if (engine.doesJavaScriptModuleExist(script)) {
            engine.runJavaScriptWithTransaction(script, trx, step.isAsync());
        }
        else
            throw new PhusionException("JS_NONE", "Failed to execute step", String.format("step=%s, script=%s",
                    step.getId(), script), ctx);
    }

    private void _executeStep(StepForEach step, Transaction trx) throws Exception {
        Context ctx = trx.getContext();
        assert ctx != null;

        if (currentForEachStep != null)
            throw new PhusionException("IT_NESTED_LOOP", "Failed to execute step", "step="+step.getId(), ctx);

        msgItemPointer = 0;
        currentForEachStep = step.getId();
        msgItems = new ArrayList<>();
        msgResultItems = new ArrayList<>();

        JSONArray msg = trx.getMessage().getJSONArray();
        if (msg!=null && msg.size()>0) {
            for (int i = 0; i < msg.size(); i++) {
                msgItems.add(new DataObject(msg.getString(i)));
            }

            Step[] nextSteps = idef.getNextSteps(step.getId());
            Step nextStep = (nextSteps==null || nextSteps.length==0) ? null : nextSteps[0];

            if (nextStep==null || nextStep instanceof StepCollect) {
                // For sake of completeness, if the next step is just a Collect step, then move the pointer right to the end

                for (int i = 0; i < msg.size(); i++) {
                    msgResultItems.add(new DataObject(msg.getString(i)));
                }
                msgItemPointer = msgItems.size();
            }
            else
                trx.setMessage( msgItems.get(msgItemPointer) );
        }
        else {
            // No item found, go to the Collect step directly
            trx.setMessage( new DataObject("[]") );
            Step nextStep = idef.getNextCollectStep(step.getId());
            if (nextStep != null) trx.moveToStep(nextStep.getId());
            else trx.moveToEnd();
        }
    }

    private void _executeStep(StepCollect step, Transaction trx) throws Exception {
        Context ctx = trx.getContext();
        assert ctx != null;

        if (currentForEachStep == null)
            throw new PhusionException("IT_NONE_LOOP", "Failed to execute step", "step="+step.getId(), ctx);

        if (msgItemPointer < msgItems.size()) {
            msgResultItems.add(trx.getMessage());
            msgItemPointer++;
        }

        if (msgItemPointer >= msgItems.size()) {
            // ForEach loop has completed

            StringBuilder result = new StringBuilder();
            result.append("[");
            for (int i = 0; i < msgResultItems.size(); i++) {
                if (i > 0) result.append(",");
                result.append(msgResultItems.get(i).getString());
            }
            result.append("]");

            trx.setMessage(new DataObject(result.toString()));

            _clearForEachLoopStates();
        }
        else {
            trx.setMessage( msgItems.get(msgItemPointer) );
            trx.moveToStep(idef.getNextSteps(currentForEachStep)[0].getId());
        }
    }

    private void _clearForEachLoopStates() {
        if (currentForEachStep != null) {
            msgItemPointer = -1;
            msgItems = null;
            msgResultItems = null;
            currentForEachStep = null;
        }
    }

}
