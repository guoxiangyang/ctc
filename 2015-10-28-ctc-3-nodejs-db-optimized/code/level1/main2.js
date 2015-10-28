'use strict';
var DB = require("./db.js");

var db = DB({
    type     : "mysql",
    host     : "192.168.88.139",
    user     : "test",
    password : "1234",
    database : "test"
});
var t = new Date();

var finished = false;

db.connect(function (err) {
    if (err) {
        console.log(err);
        db.end();
        finished = true;
    } else {
        console.log("db connected ok");
        db.query("select * from test", function (err, result) {
            console.log("[2] : ", new Date() - t);
            if (err) {
                console.log(err);
            } else {
                console.log("recotds = ", result.length);
            }
            db.end();
            finished = true;
        });
        console.log("[1] : ", new Date() - t);
    }
});

var timer = 0;
var cnt = 0;
var last_tick = new Date();
function loop() {
    if (finished) {
        clearTimeout(timer);
        return;
    }
    cnt++;
    if (new Date() - last_tick > 1000) {
        console.log("loop cnt = ", cnt);
        last_tick = new Date();
        cnt = 0;
    }
}

timer = setInterval(loop, 0);
