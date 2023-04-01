var bridge = require("../../classes/phusion/JavaBridge");

exports._runTransaction = function(strTransaction) {
    var trx = new bridge.Transaction(strTransaction);

    var storage = trx.getContext().getEngine().getFileStorageForApplication("ISimple");

    console.log( "Example.txt exists? " + storage.doesFileExist("/a/example.txt") );
    console.log( "ExampleA.txt exists? " + storage.doesFileExist("/a/exampleA.txt") );
    console.log( "Example.txt bytes? " + storage.getFileSize("/a/example.txt") );

    var content = storage.readFromFileInString("/a/example.txt");
    console.log( "Example.txt content: " + content );

    storage.saveToFile("/a/exampleCopy.txt", content);
    contentCopy = storage.readFromFileInString("/a/exampleCopy.txt");
    console.log( "Copy to ExampleCopy.txt, content: " + contentCopy );

    storage.removeFile("/a/exampleCopy.txt");
    console.log( "Remove ExampleCopy.txt: done" );

    console.log( "Example.txt url? " + storage.getPublicFileUrl("/a/example.txt") );

    var img = storage.readFromFile("/a/example.jpg");
    storage.saveToFile("/a/exampleCopy.jpg", img);
    console.log( "Copy Example.jpg: done" );

    var files = storage.listFolders("/");
    console.log( "List folders: " + JSON.stringify(files) );

    files = storage.listFiles("/a/");
    console.log( "List files: " + JSON.stringify(files) );

    return trx.toString();
};
