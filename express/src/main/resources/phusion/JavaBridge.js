
var Transaction = function(strTransaction) {
    this._data = strTransaction ? JSON.parse(strTransaction) : {};
};

Transaction.prototype = {
    EXCEPTION_STEP_ID: "exception",

    getId: function(){
        return this._data.id;
    },

    getIntegrationId: function(){
        return this._data.integrationId;
    },

    getClientId: function(){
        return this._data.clientId;
    },

    getCurrentStep: function(){
        return this._data.step;
    },

    getPreviousStep: function(){
        return this._data.stepFrom;
    },

    moveToStep: function(step){
        this._data.stepFrom = this._data.step;
        this._data.step = step;

        if (this.EXCEPTION_STEP_ID == step) this._data.failed = true;
    },

    moveToException: function(exceptionMsg){
        this._data.stepFrom = this._data.step;
        this._data.step = this.EXCEPTION_STEP_ID;
        this._data.failed = true;

        this.setProperty(this.EXCEPTION_STEP_ID, exceptionMsg);
    },

    moveToEnd: function(){
        this._data.stepFrom = this._data.step;
        this._data.step = null;
    },

    isFailed: function(){
        return this._data.failed ? true : false;
    },

    isFinished: function(){
        return this._data.step ? false : true;
    },

    getProperty: function(prop) {
        var props = this._data.properties;
        return props ? props[prop] : null;
    },

    setProperty: function(prop, value) {
        var props = this._data.properties;

        if (! props) {
          props = {};
          this._data.properties = props;
        }

        props[prop] = value;
    },

    getMessage: function() {
        var msg = this._data.msg;
        return msg ? msg : {};
    },

    setMessage: function(msg) {
        this._data.msg = msg;
    },

    getIntegrationConfig: function(key) {
        var config = this._data.config;
        if (key) return config ? config[key] : null;
        else return config;
    },

    getContext: function(){
        return new Context(this._data);
    },

    toString: function(){
        return JSON.stringify(this._data);
    }
};

var Context = function(data) {
    this._data = data;
};

Context.prototype = {
    ping: function() {
        return _callJava();
    },

    logInfo: function(position, msg, data) {
        if (! msg) msg = "";
        if (! data) data = "";
        _callJava("context", null, "logInfo", position, msg, data);
    },

    logError: function(position, msg, data) {
        if (! msg) msg = "";
        if (! data) data = "";
        _callJava("context", null, "logError", position, msg, data);
    },

    getEngine: function() {
        return new Engine(this._data);
    }
};

var Engine = function(data) {
    this._data = data;
};

Engine.prototype = {
    getKVStorageForIntegration: function(id) {
        if (!id) id = this._data.integrationId;
        return new KVStorage("integration", id);
    },

    getKVStorageForClient: function(id) {
        if (!id) id = this._data.clientId;
        return new KVStorage("client", id);
    },

    getKVStorageForApplication: function(id) {
        return new KVStorage("application", id);
    },

    getDBStorageForIntegration: function(id) {
        if (!id) id = this._data.integrationId;
        return new DBStorage("integration", id);
    },

    getDBStorageForClient: function(id) {
        if (!id) id = this._data.clientId;
        return new DBStorage("client", id);
    },

    getDBStorageForApplication: function(id) {
        return new DBStorage("application", id);
    },

    getFileStorageForIntegration: function(id) {
        if (!id) id = this._data.integrationId;
        return new FileStorage("integration", id);
    },

    getFileStorageForClient: function(id) {
        if (!id) id = this._data.clientId;
        return new FileStorage("client", id);
    },

    getFileStorageForApplication: function(id) {
        return new FileStorage("application", id);
    }
};

var FileStorage = function(type, id) {
    this._id = type+" "+id; // application, integration, client
};

FileStorage.prototype = {
    doesFileExist: function(path) {
        return _callJava("filestorage", this._id, "doesFileExist", path);
    },
    doesPublicFileExist: function(path) {
        return _callJava("filestorage", this._id, "doesPublicFileExist", path);
    },

    getFileSize: function(path) {
        return _callJava("filestorage", this._id, "getFileSize", path);
    },
    getPublicFileSize: function(path) {
        return _callJava("filestorage", this._id, "getPublicFileSize", path);
    },

    listFolders: function(path) {
        var result = _callJava("filestorage", this._id, "listFolders", path);
        if (result) return JSON.parse(result);
        else return null;
    },
    listPublicFolders: function(path) {
        var result = _callJava("filestorage", this._id, "listPublicFolders", path);
        if (result) return JSON.parse(result);
        else return null;
    },
    listFiles: function(path) {
        var result = _callJava("filestorage", this._id, "listFiles", path);
        if (result) return JSON.parse(result);
        else return null;
    },
    listPublicFiles: function(path) {
        var result = _callJava("filestorage", this._id, "listPublicFiles", path);
        if (result) return JSON.parse(result);
        else return null;
    },

    saveToFile: function(path, content) { // content can be UTF-8 String or ArrayBuffer
        _callJava("filestorage", this._id, "saveToFile", path, content);
    },
    saveToPublicFile: function(path, content) {
        _callJava("filestorage", this._id, "saveToPublicFile", path, content);
    },

    readFromFileInString: function(path) {
        return this.readFromFile(path, true);
    },

    readFromPublicFileInString: function(path) {
        return this.readFromPublicFile(path, true);
    },

    readFromFile: function(path, inString) { // If inString=true, return UTF-8 String, otherwise return ArrayBuffer. The default is false
        inString = (inString) ? "true" : "false";
        return _callJava("filestorage", this._id, "readFromFile", path, inString);
    },
    readFromPublicFile: function(path, inString) {
        inString = (inString) ? "true" : "false";
        return _callJava("filestorage", this._id, "readFromPublicFile", path, inString);
    },

    removeFile: function(path) {
        _callJava("filestorage", this._id, "removeFile", path);
    },
    removePublicFile: function(path) {
        return _callJava("filestorage", this._id, "removePublicFile", path);
    },
    removeAll: function() {
        return _callJava("filestorage", this._id, "removeAll");
    },

    getPublicFileUrl: function(path) {
        return _callJava("filestorage", this._id, "getPublicFileUrl", path);
    }
};

var KVStorage = function(type, id) {
    this._id = type+" "+id; // application, integration, client
};

KVStorage.prototype = {
    put: function(key, value, ms) { // ms is optional
        ms = (ms && ms!=0) ? ""+ms : "";
        _callJava("kvstorage", this._id, "put", key, ""+value, ms);
    },

    get: function(key) {
        return _callJava("kvstorage", this._id, "get", key);
    },

    doesExist: function(key) {
        var str = _callJava("kvstorage", this._id, "doesExist", key);

        if (str && str=="true") return true;
        else return false;
    },

    remove: function(key) {
        _callJava("kvstorage", this._id, "remove", key);
    },

    lock: function(key, ms) { // ms is optional
        ms = (ms && ms!=0) ? ""+ms : "";
        var str = _callJava("kvstorage", this._id, "lock", key, ms);

        if (str && str=="true") return true;
        else return false;
    },

    unlock: function(key) {
        _callJava("kvstorage", this._id, "unlock", key);
    }
};

var DBStorage = function(type, id) {
    this._id = type+" "+id; // application, integration, client
};

DBStorage.prototype = {
    _paramsToJSONString: function(params) {
        if (! params) return "{}";

        var obj = {};
        for (var i=0; i<params.length; i++) {
            obj[i] = params[i];
        }

        return JSON.stringify(obj);
    },

    doesTableExist: function(tableName) {
        var str = _callJava("dbstorage", this._id, "doesTableExist", tableName);

        if (str && str=="true") return true;
        else return false;
    },

    insertRecord: function(tableName, record) {
        var str = _callJava("dbstorage", this._id, "insertRecord", tableName, JSON.stringify(record));
        return parseInt(str);
    },

    queryRecords: function(tableName, selectClause, whereClause, groupClause, havingClause, params, orderClause, from, length) { // By default, from=0, length=100
        if (! selectClause) selectClause = "";
        if (! whereClause) whereClause = "";
        if (! groupClause) groupClause = "";
        if (! havingClause) havingClause = "";
        if (! orderClause) orderClause = "";
        from = (from && from!=0) ? ""+from : "";
        length = (length && length!=0) ? ""+length : "";
        params = this._paramsToJSONString(params);

        var str = _callJava("dbstorage", this._id, "queryRecords", tableName, selectClause, whereClause, groupClause, havingClause, params, orderClause, from, length);

        if (str) return JSON.parse(str);
        else return null;
    },

    queryCount: function(tableName, selectClause, whereClause, params) {
        if (! selectClause) selectClause = "";
        if (! whereClause) whereClause = "";
        params = this._paramsToJSONString(params);
        var str = _callJava("dbstorage", this._id, "queryCount", tableName, selectClause, whereClause, params);
        return parseInt(str);
    },

    queryRecordById: function(tableName, selectClause, idField, value) {
        if (! selectClause) selectClause = "";
        params = this._paramsToJSONString([value]);
        var str = _callJava("dbstorage", this._id, "queryRecordById", tableName, selectClause, idField, params);

        if (str) return JSON.parse(str);
        else return;
    },

    updateRecords: function(tableName, record, whereClause, params) {
        if (! whereClause) whereClause = "";
        params = this._paramsToJSONString(params);
        var str = _callJava("dbstorage", this._id, "updateRecords", tableName, JSON.stringify(record), whereClause, params);
        return parseInt(str);
    },

    updateRecordById: function(tableName, record, idField, value) {
        params = this._paramsToJSONString([value]);
        var str = _callJava("dbstorage", this._id, "updateRecordById", tableName, JSON.stringify(record), idField, params);
        return parseInt(str);
    },

    replaceRecordById: function(tableName, record, idField, value) {
        params = this._paramsToJSONString([value]);
        var str = _callJava("dbstorage", this._id, "replaceRecordById", tableName, JSON.stringify(record), idField, params);
        return parseInt(str);
    },

    deleteRecords: function(tableName, whereClause, params) {
        if (! whereClause) whereClause = "";
        params = this._paramsToJSONString(params);
        var str = _callJava("dbstorage", this._id, "deleteRecords", tableName, whereClause, params);
        return parseInt(str);
    },

    deleteRecordById: function(tableName, idField, value) {
        params = this._paramsToJSONString([value]);
        var str = _callJava("dbstorage", this._id, "deleteRecordById", tableName, idField, params);
        return parseInt(str);
    }
};

exports.Transaction = Transaction;
