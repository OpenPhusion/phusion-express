"use strict"

var bridge = require("../../classes/phusion/JavaBridge");

exports._runTransaction = function(strTransaction) {

    var trx = new bridge.Transaction(strTransaction);

    if (trx.getCurrentStep() == "03") {
        var msg = trx.getMessage();
        msg.text = "Hello, " + msg.text;
    }
    else if (trx.getCurrentStep() == "05") {
        var msg = trx.getMessage();

        if (msg && msg.length>0) {
            let strs = []
            for (let i=0; i<msg.length; i++) strs.push(msg[i].text);
            msg = {text: strs.join(". ")};
        }
        else msg = {text: 'nothing'};

        trx.setMessage(msg);

        trx.setProperty("a","haha");
    }

    return trx.toString();
};
