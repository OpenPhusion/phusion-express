var bridge = require("../../classes/phusion/JavaBridge");

exports._runTransaction = function(strTransaction) {
    var trx = new bridge.Transaction(strTransaction);

    var storage = trx.getContext().getEngine().getDBStorageForClient();
    var table = "Order";
//    var record = {"id":1001, "name":"Alice", "score":85.5, "male":true, "time":"2022-10-01 17:53:28"};
    var record = {"score":94, "x":"Max score"};
    var result;

//    console.log( storage.doesTableExist(table) );

//    console.log( storage.insertRecord(table, record) );

//    console.log( storage.deleteRecords(table, "name like ?", ["%六"]) );
//    console.log( storage.deleteRecordById(table, "id", 1006) );

//    console.log( storage.updateRecords(table, record, "male and time<?", ["2022-10-03"]) );
//    console.log( storage.updateRecordById(table, record, "id", 1005) );
//    console.log( storage.replaceRecordById(table, record, "id", 1005) );

//    result = storage.queryRecordById(table, "name, score", "id", "1003");
//    console.log( result ? JSON.stringify(result) : "Not found" );

//    console.log( storage.queryCount(table, "distinct male", null, null) );
//    console.log( storage.queryCount(table, null, "name like ?", ["王%"]) );

    result = storage.queryRecords(table, "name, score", null, null, null, null, "score", 1, 2);
    console.log( JSON.stringify(result) );

    return trx.toString();
};
