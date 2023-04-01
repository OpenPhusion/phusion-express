var bridge = require("../../classes/phusion/JavaBridge");
var def = require("../../classes/phusion/IntegrationDef");

exports._run = function() {
    try {
        var result = def.IntegrationDefStart.

            // in({endpoint:"fa.a", connection:"fa"})
            timer({interval:10})
                .msg({})
                .out({endpoint:"aa.bb", connection:"bb"})
                .java({module:"a", class:"b"})
                .js("04",{async:true})
                .branch()
                    .js()
                .end()
                .branch()
                    .js()
                .end()
                .foreach()
                    .js()
                .end()
            .error()
                .msg({})
            .done();

        if (typeof(result) != "string") throw new Error("Must end with done()");
        return result;
    } catch(e) {
        var str = e.name+": "+e.message;
        if (str.indexOf("not a function") > 0) str += ". Must start with in() or timer()";
        return str;
    }
};
