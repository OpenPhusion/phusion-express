var bridge = require("../../classes/phusion/JavaBridge");

// Global imports and functions

var md5 = require("md5-node");

exports._runTransaction = function(strTransaction) {

    // Step-wise function

    var trx = new bridge.Transaction(strTransaction);

//    console.log( trx.getContext().ping() );
//    trx.getContext().logError("JSBridgeTest", "Test log an error");

    var msg = trx.getMessage();
    msg.digest = md5(msg.text);
    return trx.toString();
};
