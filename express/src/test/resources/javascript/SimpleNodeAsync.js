var bridge = require("../../classes/phusion/JavaBridge");

exports._run = function(callbackHandle) {

    setTimeout(function(){
        _callback(callbackHandle, "hello");
    }, 200);

};
