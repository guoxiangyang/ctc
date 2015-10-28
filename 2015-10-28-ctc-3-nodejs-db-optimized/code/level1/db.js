'use strict';

var util = require("util");

function NOT_IMPLEMENTATION_YET() {
    console.log("NOT IMPLEMENTATION YET.");
    throw "Not Implementation.";
}

function TC_DB(config) {
    this.config = config;
}
TC_DB.prototype.query = NOT_IMPLEMENTATION_YET;
TC_DB.prototype.connect = function (callback) {
    var err = "call virtual method";
    callback(err);
};

function TC_DB_MYSQL(config) {
    TC_DB.call(this, config);
    this.config.multipleStatements = true;
    this.mysql  = require("mysql");
    this.connection = null;
}
util.inherits(TC_DB_MYSQL, TC_DB);

TC_DB_MYSQL.prototype.end = function () {
    if (this.connection) {
        this.connection.end();
        this.connection = null;
    }
};
TC_DB_MYSQL.prototype.connect = function (callback) {
    this.connection = this.mysql.createConnection(this.config);
    this.connection.connect(function (err) {
        callback(err);
    }.bind(this));
    this.connection.on('error', function (err) {
        console.log('db error', err);
        if (err.code === 'PROTOCOL_CONNECTION_LOST') {
            this.server.make_db_connection();
        } else {
            throw err;
        }
    }.bind(this));
};
TC_DB_MYSQL.prototype.query = function (sql, param, callback) {
    // console.log("SQL   = ", sql);
    // console.log("PARAM = ", param);
    this.connection.query(sql, param, function (err, recordsets) {
	    if (err) {
            callback(err, recordsets);
	    } else {
	        if (Array.isArray(recordsets[0])) {
		        console.log("====== multi result =====");
		        callback(err, recordsets);
	        } else {
		        console.log("====== single result =====");
		        // console.log(recordsets);
		        callback(err, [recordsets]);
	        }
	    }
    });
};


function TC_DB_MSSQL(config) {
    if (!config.options) {
        config.options = {};
    }
    config.options.useUTC = false;
    TC_DB.call(this, config);
    this.mssql = require("mssql");
    this.connection = null;
}
util.inherits(TC_DB_MSSQL, TC_DB);



module.exports = function (config) {
    if (config.type === "mssql") {
        return new TC_DB_MSSQL(config);
    } else if (config.type === "mysql") {
        return new TC_DB_MYSQL(config);
    }
};

