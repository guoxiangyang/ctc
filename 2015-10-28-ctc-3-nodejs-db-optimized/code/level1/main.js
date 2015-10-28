'use strict';
var DB = require("./db.js");

var db = DB({
    type     : "mysql",
    host     : "192.168.88.139",
    user     : "test",
    password : "1234",
    database : "test"
});
db.connect(function (err) {
    if (err) {
        console.log(err);
        db.end();
    } else {
        console.log("db connected ok");
        db.query("select * from test", function (err, result) {
            if (err) {
                console.log(err);
            } else {
                console.log(result.length);
            }
            db.end();
        });
    }
});
