
"use strict"

var bridge = require("../../classes/phusion/JavaBridge");

// Global imports and functions

var md5 = require("md5-node");

exports._runTransaction = function(strTransaction) {
    var trx = new bridge.Transaction(strTransaction);
    var msg = trx.getMessage();
    msg.digest = md5(msg.text);

    if (trx.getCurrentStep() != "exception") {
        var aa = bb.cc();
    }

    return trx.toString();
};
