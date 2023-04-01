//var bridge = require("../../classes/phusion/JavaBridge");
var commons = require("../../classes/phusion/PhusionCommons");

exports._run = function() {
//    var ctx = (new bridge.Transaction()).getContext();
//
//    var storage = ctx.getEngine().getKVStorageForIntegration("A");
//    storage.put("test", "hello, world", 1000);
//    console.log(storage.get("test"));

    var shift = 5*60*60*1000;
    return commons.TimeUtil.timestampToStr10( 0, shift );

//    return "hello";
};
