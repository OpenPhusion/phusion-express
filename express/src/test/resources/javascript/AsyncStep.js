
"use strict"

var bridge = require("../../classes/phusion/JavaBridge");

// Global imports and functions

exports._runTransaction = function(strTransaction, callbackHandle) {

    var trx = new bridge.Transaction(strTransaction);
    var msg = trx.getMessage();

    if (trx.getCurrentStep() == "01") {
        msg = {name: "sunyu"};
        trx.setMessage(msg);
    }
    else {
        msg.age = 30;
    }

    setTimeout(function(){
        _callback(callbackHandle, trx.toString());
    }, 200);

};
