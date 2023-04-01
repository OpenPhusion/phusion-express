var bridge = require("../../classes/phusion/JavaBridge");

function sleep(time) {
    var timeStamp = new Date().getTime();
    var endTime = timeStamp + time;
    while (true) {
        if (new Date().getTime() > endTime) return;
    }
}

exports._runTransaction = function(strTransaction) {
    var trx = new bridge.Transaction(strTransaction);

    var storage = trx.getContext().getEngine().getKVStorageForIntegration();
    var key = "carNumber";
    var value = "123";
    var time = 1000;

    console.log("Exist? "+storage.doesExist(key));
    console.log("Set to "+value+" for "+time+"ms");
    storage.put(key, value, time);
    console.log("Now exist? "+storage.doesExist(key));
    console.log("What? "+storage.get(key));
    console.log("Wait a while...");
    sleep(time);
    console.log("Now, exist? "+storage.doesExist(key));

    var lock = storage.lock("A");
    console.log("Lock? "+lock);
    if (lock) {
        storage.unlock("A");
        console.log("Unlocked");
    }

    return trx.toString();
};
