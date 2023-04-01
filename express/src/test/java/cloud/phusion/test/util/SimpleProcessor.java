package cloud.phusion.test.util;

import cloud.phusion.DataObject;
import cloud.phusion.integration.Processor;
import cloud.phusion.integration.Transaction;

public class SimpleProcessor implements Processor {
    @Override
    public void process(Transaction trx) throws Exception {
        String msg = trx.getMessage().getString();
        if (msg == null) msg = "Nothing";

        trx.setMessage(new DataObject(msg + " | Processed @" + Thread.currentThread().getName()));
    }
}
