/*
    Must start with one of the triggers:
    - in(ops, desc?), waiting for incoming endpoint message. "ops" can be {endpoint:"application_id.endpoint_id"}.
    - timer(ops, desc?), periodical execution. "ops" can be {interval:10, cron:"cron_string", clustered:true}, where "interval" is in seconds, and there must be one and only one of "interval" and "cron".

    Processing nodes:
    - .msg(id?, msg, desc?), sending message directly. "msg" must be an JSON object.
    - .out(id?, ops, desc?), calling outbound endpoint. "ops" can be {endpoint:"application_id.endpoint_id"}.
    - .java(id?, ops, desc?), executing Java code. "ops" can be {module:"java_module", class:"full_java_class_name"}, where the Java class must implement interface cloud.phusion.integration.Processor.
    - .js(id?, ops?, desc?), executing JavaScript code. "ops" can be {async:false}.

    Controlling nodes:
    - .branch(), starting a new branch of sub-workflow (just like a "case" in a "switch" statement).
    - .foreach(), when the message contains an array of items, executing the sub-workflow for each item (item-by-item sequentially).
    - .end(), ending a branch or a "foreach".

    Must have an error node:
    - .error(), which can be followed with any processing node (whose id must be "exception").

    Must end with:
    - .done();

    Note:
    - "param?" means this parameter is optional. If "id" is not specified, it will be automatically assigned an ID.
    - The "branch" and "end" nodes will be automatically filled with properties "branchFrom" (the processing node before the branch) and "branchEnds" (the last processing nodes in all parallel branches).
    - The "end" for a "foreach" will be automatically renamed as "collect".
*/

var Util = {
    _patternId: /^[-0-9a-zA-Z_\\.]*$/,

    checkOps: function(ops, propsRequired, propsOptional, canBeEmpty) {
        var msg = "Parameter \"ops\" is required and must be an object";
        if (propsRequired.length > 0)
            msg += " with properties " + propsRequired.join(", ") + " (required)";
        if (propsOptional.length > 0) {
            if (propsRequired.length > 0) msg += " and ";
            else msg += " with properties ";
            msg += propsOptional.join(", ") + " (optional)";
        }

        if (!canBeEmpty && !ops) throw new Error(msg);
        if (typeof(ops) != "object") throw new Error(msg);

        propsRequired.forEach((prop, i) => {
            if (! ops[prop]) throw new Error(msg);
        });
    },

    checkPrevError: function(list, obj) {
        var prev = list[list.length-1];
        if (prev.node == "error") {
            if (obj.id && obj.id != "exception")
                throw new Error("The node immediately after error() must be named \"exception\"");
            obj.id = "exception";
        }
    },

    getParams: function(node, id, ops, desc) {
        var obj = {node:node};

        if (id) {
            if (typeof(id) == "object") {
                obj.ops = id;
                obj.desc = ops;
            }
            else obj.id = id;
        }

        if (ops) {
            if (typeof(ops) == "object") {
                obj.ops = ops;
                obj.desc = desc;
            }
            else obj.desc = ops;
        }

        if (desc) obj.desc = desc;

        var str = obj.id;
        var msg = "\"id\" should be sequence of (no more than 20) digits, letters, dashes, underscores, points";
        if (str) {
            if (str.length > 20) throw new Error(msg);
            if (! Util._patternId.test(str)) throw new Error(msg);
        }

        return obj;
    },

    fillId: function(def, obj) {
        def._id ++;
        if (! obj.id) {
            obj.id = def._id > 9 ? ""+def._id : "0"+def._id;
        }
    },

    composeNodeTimer: function(obj, prev) {
        return obj.ops;
    },

    _composeCommon: function(obj, prev) {
        var from;
        if (prev) {
            if (prev.node == "branch") from = prev.branchFrom;
            else if (prev.node == "end") from = prev.branchEnds;
            else if (prev.id) from = prev.id;
        }
        return {id:obj.id, desc:obj.desc, from:from};
    },

    _composeNodeEndpoint: function(direction, obj, prev) {
        var result = Util._composeCommon(obj, prev);
        result.type = "endpoint";
        result.direction = direction;

        var endpoint = obj.ops.endpoint;
        var pos = endpoint.indexOf(".");
        result.app = endpoint.substring(0,pos);
        result.endpoint = endpoint.substring(pos+1);
        return result;
    },

    composeNodeIn: function(obj, prev) {
        return Util._composeNodeEndpoint("in", obj, prev);
    },

    composeNodeOut: function(obj, prev) {
        return Util._composeNodeEndpoint("out", obj, prev);
    },

    composeNodeMsg: function(obj, prev) {
        var result = Util._composeCommon(obj, prev);
        result.type = "direct";
        result.msg = obj.ops;
        return result;
    },

    composeNodeJava: function(obj, prev) {
        var result = Util._composeCommon(obj, prev);
        result.type = "processor";
        result.subtype = "java";
        result.module = obj.ops.module;
        result.class = obj.ops.class;
        return result;
    },

    composeNodeJS: function(obj, prev) {
        var result = Util._composeCommon(obj, prev);
        result.type = "processor";
        result.subtype = "javascript";
        if (obj.ops) {
            result.async = obj.ops.async;
        }
        return result;
    },

    composeNodeForeach: function(obj, prev) {
        var result = Util._composeCommon(obj, prev);
        result.type = "forEach";
        return result;
    },

    composeNodeCollect: function(obj, prev) {
        var result = Util._composeCommon(obj, prev);
        result.type = "collect";
        return result;
    }
};

var IntegrationDef = function(step){
    this._id = 0;
    this._stack = [];
    this._list = [step];
};

IntegrationDef.prototype = {
    msg: function(id, ops, desc){
        var obj = Util.getParams("msg", id, ops, desc);
        Util.checkOps(obj.ops, [], []);
        Util.checkPrevError(this._list, obj);
        Util.fillId(this, obj);
        this._list.push(obj);
        return this;
    },

    out: function(id, ops, desc){
        var obj = Util.getParams("out", id, ops, desc);
        Util.checkOps(obj.ops, ["endpoint"], []);
        if (obj.ops.endpoint.indexOf(".") <= 0)
            throw new Error("Endpoint shoulde be \"applicationId.endpointId\"");
        Util.checkPrevError(this._list, obj);
        Util.fillId(this, obj);
        this._list.push(obj);
        return this;
    },

    java: function(id, ops, desc){
        var obj = Util.getParams("java", id, ops, desc);
        Util.checkOps(obj.ops, ["module","class"], []);
        Util.checkPrevError(this._list, obj);
        Util.fillId(this, obj);
        this._list.push(obj);
        return this;
    },

    js: function(id, ops, desc){
        var obj = Util.getParams("js", id, ops, desc);
        Util.checkPrevError(this._list, obj);
        Util.fillId(this, obj);
        this._list.push(obj);
        return this;
    },

    branch: function(){
        var obj = this._list[this._list.length - 1];
        var nodeName = obj.node;

        if (nodeName!="java" && nodeName!="js" && nodeName!="end")
            throw new Error("Branch must follow java() or js() or end()");

        var objBranch = {
            node: "branch",
            branchFrom: (obj.id ? obj.id : obj.branchFrom),
            branchEnds: obj.branchEnds
        };

        this._stack.push(objBranch);
        this._list.push(objBranch);
        return this;
    },

    foreach: function(){
        this._stack.push({node:"foreach"});

        var obj = {node:"foreach"};
        Util.fillId(this, obj);
        this._list.push(obj);
        return this;
    },

    end: function(){
        var prev = this._stack.pop();
        if (prev) {
            if (prev.node == "foreach") {
                var obj = {node:"collect"};
                Util.fillId(this, obj);
                this._list.push(obj);
            }
            else { // prev.node == "branch"
                var ends = prev.branchEnds;
                if (!ends) ends = [];
                var prevInList = this._list[this._list.length - 1];
                if (prevInList.id) ends.push(prevInList.id);

                var obj = {
                    node: "end",
                    branchFrom: prev.branchFrom,
                    branchEnds: ends
                };
                this._list.push(obj);
            }
        }
        return this;
    },

    error: function(){
        this._list.push({node:"error"});
        return this;
    },

    done: function(){
        var result = {workflow:[]};

        var list = this._list;
        var prev = null;

        for (var i = 0; i < list.length; i++) {
            var obj = list[i];

            switch (obj.node) {
                case "timer": result.timer = Util.composeNodeTimer(obj, prev); break;
                case "in": result.workflow.push(Util.composeNodeIn(obj, prev)); break;
                case "out": result.workflow.push(Util.composeNodeOut(obj, prev)); break;
                case "msg": result.workflow.push(Util.composeNodeMsg(obj, prev)); break;
                case "java": result.workflow.push(Util.composeNodeJava(obj, prev)); break;
                case "js": result.workflow.push(Util.composeNodeJS(obj, prev)); break;
                case "foreach": result.workflow.push(Util.composeNodeForeach(obj, prev)); break;
                case "collect": result.workflow.push(Util.composeNodeCollect(obj, prev)); break;
            }

            prev = obj;
        }

        return JSON.stringify(result);
    }
};

var IntegrationDefStart = {
    in: function(ops, desc){
        Util.checkOps(ops, ["endpoint"], []);
        if (ops.endpoint.indexOf(".") <= 0)
            throw new Error("Endpoint shoulde be \"applicationId.endpointId\"");
        return new IntegrationDef({node:"in", id:"start", ops:ops, desc:desc});
    },

    timer: function(ops, desc){
        Util.checkOps(ops, [], ["interval","cron","clustered"]);
        if (!ops.interval && !ops.cron)
            throw new Error("Interval or cron must be defined");
        return new IntegrationDef({node:"timer", ops:ops, desc:desc});
    }
};

exports.IntegrationDefStart = IntegrationDefStart;
