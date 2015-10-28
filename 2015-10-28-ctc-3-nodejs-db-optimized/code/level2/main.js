'use strict';
var DB = require("./db.js");

var db = DB({
    type     : "mysql",
    host     : "192.168.88.139",
    user     : "test",
    password : "1234",
    database : "test"
});

var cnt = 1000000;
var cur = 0;
function do_insert_data() {
    var sql = "insert into test values("
        + cur + ","
        + '"' + cur + '")';
    db.query(sql, function (err, result) {
        if (err) {
            console.log(err);
            db.end();
        } else {
            if (cnt % 10000 === 0) {
                console.log(new Date(), cur, "inserted");
            }
            if (cnt > 0) {
                cnt--;
                cur++;
                do_insert_data();
            } else {
                db.end();
            }
        }
    });
}

function get_max_id() {
    var sql = "select max(`key`) + 1 as max from test";
    db.query(sql, function (err, result) {
        if (err) {
            console.log(err);
            db.end();
        } else {
            console.log("max id = ", result[0].max);
            cur = result[0].max;
            do_insert_data();
        }
    });
}

function on_connected(err) {
    if (err) {
        console.log(err);
        db.end();
    } else {
        console.log("db connected ok");
        get_max_id();
    }
}

db.connect(on_connected);
