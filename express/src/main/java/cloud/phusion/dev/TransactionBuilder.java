package cloud.phusion.dev;

import cloud.phusion.Context;
import cloud.phusion.DataObject;
import cloud.phusion.Engine;
import cloud.phusion.EngineFactory;
import cloud.phusion.integration.Transaction;

public class TransactionBuilder {
    private String config = null;
    private String fromStep = null;
    private String step = null;
    private Engine engine = null;

    public TransactionBuilder setIntegrationConfig(String config) {
        this.config = config;
        return this;
    }

    public TransactionBuilder setPreviousStep(String step) {
        this.fromStep = step;
        return this;
    }

    public TransactionBuilder setCurrentStep(String step) {
        this.step = step;
        return this;
    }

    public TransactionBuilder setEngine(Engine engine) {
        this.engine = engine;
        return this;
    }

    public Transaction done() throws Exception {
        Context ctx = EngineFactory.createContext(engine);

        Transaction trx = new Transaction(
                "IT_" + engine.generateUniqueId(null),
                "" + engine.generateUniqueId(null),
                step,
                fromStep,
                ctx
        );

        trx.setIntegrationConfig(new DataObject(config));
        trx.setClientId("C_" + engine.generateUniqueId(null));

        return trx;
    }

}
