'use strict';
/*jslint maxlen:160, vars: true, plusplus:true, stupid:true*/

var util = require("util");
var bcrypt  = require('bcrypt');

function quickdiff(q_cur, q_last) {
    var i;
    var cur  = 0;
    var last = 0;
    var result = {
        removed   : [],
        added     : [],
        unchanged : []
    };
    while ((cur < q_cur.length) && (last < q_last.length)) {
        var pos;
        if (q_cur[cur] !== q_last[last]) {
            pos = q_last.indexOf(q_cur[cur]);
            if (pos >= 0) {
                // result.removed =
                //     result.removed.concat(q_last.slice(last, pos));
                for (i = last; i < pos; i++) {
                    result.removed.push(i);
                }
                last = pos;
            } else {
                // result.added.push(q_cur[cur]);
                result.added.push(cur);
                cur++;
            }
        } else {
            result.unchanged.push({
                cur  : cur,
                last : last
            });
            cur++;
            last++;
        }

        if ((cur >= q_cur.length) || (last >= q_last.length)) { break; }

        if (q_cur[cur] !== q_last[last]) {
            pos = q_cur.indexOf(q_last[last]);
            if (pos >= 0) {
                // result.added = result.added.concat(q_cur.slice(cur, pos));
                for (i = cur; i < pos; i++) {
                    result.added.push(i);
                }
                cur = pos;
            } else {
                // result.removed.push(q_last[last]);
                result.removed.push(last);
                last++;
            }
        } else {
            result.unchanged.push(cur);
            cur++;
            last++;
        }
    }
    if (cur < q_cur.length) {
        for (i = cur; i < q_cur.length; i++) {
            result.added.push(i);
        }
    }
    if (last < q_last.length) {
        for (i = last; i < q_last.length; i++) {
            result.removed.push(i);
        }
    }
    return result;
}

function NOT_IMPLEMENTATION_YET() {
    console.log("NOT IMPLEMENTATION YET.");
    throw "Not Implementation.";
}

function TC_DB(server, config) {
    console.log("db config = ", config);
    this.server = server;
    this.config = config;
    this.cache  = server.runtime.cache;
}

function gen_keys(arr, field) {
    var result = [];
    var i, j;
    for (i = 0; i < arr.length; i++) {
        var key = "";
        if (Array.isArray(field)) {
            for (j = 0; j < field.length; j++) {
                if (key) {
                    key = key + "|";
                }
                key = key + arr[i][field[j]];
            }
        } else {
            key = arr[i][field];
        }
        result.push(key);
    }
    return result;
}
function gen_sync_keys(things, field) {
    var result = [];
    var i;
    for (i = 0; i < things.length; i++) {
        if (things[i].sync_with) {
            result.push(things[i].tid + "|" + things[i].sync_with);
        }
    }
    return result;
}
TC_DB.prototype.clean_data = function () {
    var i, tid, found;
    // 1. xref use tid not exists;
    for (i = this.cache.data.xref.length - 1; i >= 0; i--) {
        found = true;
        tid = this.cache.data.xref[i].owner_tid;
        if (this.cache.keys.things.indexOf(tid) < 0) { found = false; }
        tid = this.cache.data.xref[i].tid;
        if (this.cache.keys.things.indexOf(tid) < 0) { found = false; }
        if (!found) {
            console.log("[clean data]", tid, " not found for xref.");
            this.cache.data.xref.splice(i, 1);
        }
    }
    // 2. uses tid not exists;
    for (i = this.cache.data.users.length - 1; i >= 0; i--) {
        found = true;
        tid = this.cache.data.users[i].tid;
        if (this.cache.keys.things.indexOf(tid) < 0) { found = false; }
        if (!found) {
            console.log("[clean data]", tid, " user doesn't has valid things rec.");
            this.cache.data.users.splice(i, 1);
        }
    }
    // 3. sync with tid not exists / not in use;
    for (i = this.cache.data.things.length - 1; i >= 0; i--) {
        found = true;
        tid = this.cache.data.things[i].sync_with;
        if (tid && this.cache.keys.things.indexOf(tid) < 0) {
            found = false;
        }
        if (!found) {
            console.log("[clean data]", tid, " sync with invalid tid.");
            this.cache.data.things[i].sync_with = "";
        }
    }
};
TC_DB.prototype.connect = function (callback) {
    var err = null;
    callback(err);
};
 
TC_DB.prototype.save_new_message       = NOT_IMPLEMENTATION_YET;
TC_DB.prototype.get_tid_by_fkey        = NOT_IMPLEMENTATION_YET;
TC_DB.prototype.get_in_use_things_list = NOT_IMPLEMENTATION_YET;
TC_DB.prototype.get_msg_of             = NOT_IMPLEMENTATION_YET;
TC_DB.prototype.get_parts              = NOT_IMPLEMENTATION_YET;
TC_DB.prototype.get_profile            = NOT_IMPLEMENTATION_YET;
TC_DB.prototype.get_remote_ip          = NOT_IMPLEMENTATION_YET;
TC_DB.prototype.set_private_profile    = NOT_IMPLEMENTATION_YET;
TC_DB.prototype.set_public_profile     = NOT_IMPLEMENTATION_YET;

TC_DB.prototype.auto_reg_tid = function (login_data, callback) {
    if (!login_data.tid) { callback("tid not in login_data"); }
    if ((typeof login_data.userdata) === "undefined") {
        callback("userdata not  in login_data");
    }
    var ukey;
    try {
        ukey = JSON.parse(login_data.userdata);
        if (typeof ukey === "object") {
            if (ukey.foreignkey) {
                ukey = ukey.foreignkey;
            } else {
                ukey = login_data.userdata;
            }
        } else {
            ukey = login_data.userdata;
        }
    } catch (error) {
        ukey = login_data.userdata;
    }
    ukey = ukey.toString();
    // todo: get part to bind from config file
    var parts_to_bind;
    if (this.server.config.auto_reg_bind_to) {
        parts_to_bind = this.server.config.auto_reg_bind_to;
    } else {
        parts_to_bind = ['100', '101', '102', '103'];
        /*
          [ 100 ] 警讯中心接口
          [ 101 ] IPR
          [ 102 ] 图片服务器
          [ 103 ] NVS服务器
         */
    }
    // find all tid  to bind by part_id
    var tid;
    var bind_to = [];
    var things = this.server.runtime.things;
    for (tid in things) {
        if (things.hasOwnProperty(tid)) {
            var pid = things[tid].pub.part_id;
            if (parts_to_bind.indexOf(pid) >= 0) {
                bind_to.push(tid);
            }
        }
    }
    tid = login_data.tid;
    console.log("[auto reg] ", tid, " bind to :", bind_to);
    var sql = "insert into things (tid, type, part_id, foreignkey, in_use, name) \n";
    sql = sql + "   values ('" + tid + "', 'device', 0, '" + ukey + "', 1, '[自动注册]" + tid + "'); \n";
    var i;
    for (i = 0; i < bind_to.length; i++) {
        sql = sql + " insert into things_xref (owner_tid, tid) values ('" + bind_to[i]  + "', '" + tid + "'); \n";
    }
    this.query(sql, [], function (err, recordsets) {
        if (err) {
            console.log("[error update things runtime]", err);
            callback(err);
        } else {
            this.load_data_to_runtime(callback);
        }
    }.bind(this));
};
TC_DB.prototype.set_user_password = function (param, callback) {
    console.log("[set user password] param = ", param);
    var err = false;
    if (!param) { err = true; }
    if (!param.tid) { err = true; }
    if (!param.old_pass) { err = true; }
    if (!param.new_pass) { err = true; }
    var user;
    if (!err) {
        user = this.server.runtime.users[param.tid];
        if (!bcrypt.compareSync(param.old_pass, user.password)) {
            err = true;
        }
    }
    if (err) {
        callback(err);
    } else {
        var salt = bcrypt.genSaltSync(10);
        var hash = bcrypt.hashSync(param.new_pass, salt);
        var sql = "update users set password = '" + hash + "' where tid = '" + param.tid + "'";
        console.log("salt = ", salt, "  hassh = ", hash, "  sql = ", sql);
        this.query(sql, [], function (err, recordsets) {
            if (err) {
                console.log("[error update things runtime]", err);
                callback(err);
            } else {
                user.password = hash;
                callback();
            }
        }.bind(this));
    }
};
TC_DB.prototype.shrink_database = function (callback) {
    if (!this.server.config.message_limit_cnt) { return; }
    var sql = "";
    var days = this.server.config.message_limit_cnt.toString();
    if (this.config.type === "mysql") {
        sql = "select max(mid) as mid from message where time < (NOW() - INTERVAL " + days + " DAY);";
    } else if (this.config.type === "mssql") {
        sql = "select max(mid) as mid from message where time < dateadd(Day, -" + days + " ,getdate());";
    } else {
        callback("unknown database type : " + this.config.type);
        return;
    }
    this.query(sql, [], function (err, recordsets) {
        if (err) {
            console.log("[auto delete fail] ", err);
            if (typeof callback === "function") { callback(err); }
        } else {
            var mid = recordsets[0][0].mid;
            if (mid) {
                sql = "";
                sql = sql + "delete from message where mid <= " + mid + ";";
                sql = sql + "delete from mailbox where mid not in (select mid from message)";
                this.query(sql, [], function (err, recordsets) {
                    if (err) {
                        console.log("[auto delete fail] ", err);
                        if (typeof callback === "function") { callback(err); }
                    } else {
                        if (typeof callback === "function") { callback(); }
                    }
                }.bind(this));
            } else {
                if (typeof callback === "function") { callback(); }
            }
        }
    }.bind(this));
};
TC_DB.prototype.check_update_changed = function (callback) {
    var sql = "select db_changed from local_configuration ;";
    this.query(sql, [], function (err, recordsets) {
        if (err) {
            console.log(err);
            return;
        }
        var changed;
        if (Array.isArray(recordsets[0])) {
            changed = parseInt(recordsets[0][0].db_changed, 10);
        } else {
            changed = parseInt(recordsets[0].db_changed, 10);
        }
        if (changed) {
            var sql = "update local_configuration set db_changed=0";
            console.log(sql);
            this.query(sql, [], function (err, recordsets) {
                if (err) {
                    console.log(err);
                    return;
                }
                this.load_data_to_runtime(function (err) {
                    callback(changed);
                });
            }.bind(this));
        } else {
            callback(changed);
            this.shrink_database();
        }
    }.bind(this));
};

TC_DB.prototype.do_smooth_call = function (funcs, callback) {
    if (!Array.isArray(funcs)) {
        callback({error: "invalid parameter of do smooth call"});
        return;
    }
    if (funcs.length === 0) {
        callback();
        return;
    }
    var func = funcs[0];
    if (typeof func === "function") {
        // console.log("call function: ", func.name);
        var t = new Date();
        var ret = func();
        if (!ret) {funcs.shift(); }
        // console.log("func cost time: ", new Date() - t, "ms",
        //            "return = ", ret);
    } else {
        console.log("not a function : ", func);
    }
    setTimeout(function () {
        this.do_smooth_call(funcs, callback);
    }.bind(this), 0);
};
TC_DB.prototype.do_smooth_update_cache = function (callback) {
    console.log("[do_smooth_update_cache]");
    this.cache.tmp = {};
    this.do_smooth_call([
        function () {
            console.log("[smooth load] gen keys of parts");
            this.cache.keys.parts
                = gen_keys(this.cache.data.parts, "part_id");
        }.bind(this),
        function () {
            console.log("[smooth load] gen keys of users");
            this.cache.keys.users
                = gen_keys(this.cache.data.users, "tid");
        }.bind(this),
        function () {
            console.log("[smooth load] gen keys of things");
            this.cache.keys.things
                = gen_keys(this.cache.data.things, "tid");
            var i;
            var len = this.cache.data.things.length;
            this.cache.keys.things_object = {};
            for (i = 0; i < len; i++) {
                var tid = this.cache.data.things[i].tid;
                this.cache.keys.things_object[tid] = true;
            }
        }.bind(this),
        function clean_data_for_xref_to_invalid_tid() {
            var i, tid, found;
            // 1. xref use tid not exists;
            if (!this.cache.tmp.clean_data_xref_index) {
                this.cache.tmp.clean_data_xref_index =
                    this.cache.data.xref.length - 1;
            }
            var from = this.cache.tmp.clean_data_xref_index;
            var to   = this.cache.tmp.clean_data_xref_index - 2000;
            if (to < 0) { to = 0; }
            this.cache.tmp.clean_data_xref_index = to;
            for (i = from; i >= to; i--) {
                found = true;
                tid = this.cache.data.xref[i].owner_tid;
                if (!this.cache.keys.things_object[tid]) {
                    found = false;
                }
                tid = this.cache.data.xref[i].tid;
                if (!this.cache.keys.things_object[tid]) {
                    found = false;
                }
                if (!found) {
                    console.log("[clean data]", tid, " not found for xref.");
                    this.cache.data.xref.splice(i, 1);
                }
            }
            return to;
        }.bind(this),
        function clean_data_for_user_tid_not_exists() {
            console.log("clean_data_for_user_tid_not_exists");
            var i, tid, found;
            // 2. uses tid not exists;
            if (!this.cache.tmp.clean_data_user_tid_index) {
                this.cache.tmp.clean_data_user_tid_index =
                    this.cache.data.users.length - 1;
            }
            var from = this.cache.tmp.clean_data_user_tid_index;
            var to   = this.cache.tmp.clean_data_user_tid_index - 2000;
            if (to < 0) { to = 0; }
            this.cache.tmp.clean_data_user_tid_index = to;
            for (i = from; i >= to; i--) {
                found = true;
                tid = this.cache.data.users[i].tid;
                if (!this.cache.keys.things_object[tid]) {
                    found = false;
                }
                if (!found) {
                    console.log("[clean data]", tid,
                                " user doesn't has valid things rec.");
                    this.cache.data.users.splice(i, 1);
                }
            }
            return to;
        }.bind(this),
        function clean_data_for_sync_invalid_tid() {
            console.log("clean_data_for_sync_invalid_tid");
            var i, tid, found;
            // 3. sync with tid not exists / not in use;
            if (!this.cache.tmp.clean_data_sync_tid_index) {
                this.cache.tmp.clean_data_sync_tid_index =
                    this.cache.data.things.length - 1;
            }
            var from = this.cache.tmp.clean_data_sync_tid_index;
            var to   = this.cache.tmp.clean_data_sync_tid_index - 2000;
            if (to < 0) { to = 0; }
            this.cache.tmp.clean_data_sync_tid_index = to;
            for (i = from; i >= to; i--) {
                found = true;
                tid = this.cache.data.things[i].sync_with;
                if (tid && !this.cache.keys.things_object[tid]) {
                    found = false;
                }
                if (!found) {
                    console.log("[clean data]", tid,
                                " sync with invalid tid.");
                    this.cache.data.things[i].sync_with = "";
                }
            }
            return to;
        }.bind(this),
        function gen_keys_for_xref() {
            console.log("gen_keys_for_xref");
            this.cache.keys.xref = gen_keys(this.cache.data.xref,
                                     ["owner_tid", "tid"]);
        }.bind(this),
        function gen_keys_for_sync() {
            console.log("gen_keys_for_sync");
            this.cache.keys.sync = gen_sync_keys(this.cache.data.things);
        }.bind(this),
        function sort_syncs() {
            console.log("sort_syncs");
            this.cache.keys.sync.sort();
        }.bind(this),
        function diff_things() {
            this.cache.diff.things = quickdiff(this.cache.keys.things,
                                               this.cache.keys_last.things);
        }.bind(this),
        function diff_users() {
            this.cache.diff.users  = quickdiff(this.cache.keys.users,
                                       this.cache.keys_last.users);
        }.bind(this),
        function diff_parts() {
            this.cache.diff.parts = quickdiff(this.cache.keys.parts,
                                              this.cache.keys_last.parts);
        }.bind(this),
        function diff_xref() {
            this.cache.diff.xref = quickdiff(this.cache.keys.xref,
                                             this.cache.keys_last.xref);
        }.bind(this),
        function diff_sync() {
            this.cache.diff.sync  = quickdiff(this.cache.keys.sync,
                                              this.cache.keys_last.sync);
        }.bind(this),
        this.update_step_1_xref_del.bind(this),
        this.update_step_2_sync_del.bind(this),
        this.update_step_3_things_disable.bind(this),
        this.update_step_4_things_del.bind(this),
        this.update_step_5_general_update.bind(this),
        this.update_step_6_things_new.bind(this),
        this.update_step_7_things_enable.bind(this),
        this.update_step_8_sync_add.bind(this),
        this.update_step_9_xref_add.bind(this),
        this.update_step_10_update_things.bind(this),
        this.update_step_11_update_parts.bind(this),
        this.update_step_12_update_users.bind(this),
        function () {
            // 4. add myself is not exists;
            var tid = this.server.config.tid;
            if (!this.server.runtime.things[tid]) {
                this.server.runtime.things[tid] = {
                    priv : {
                        addr_type     : "",
                        sync_with     : "",
                        sync_by       : [],
                        addr_template : "",
                        profile       : "",
                        seqnum        : 1,
                        unreads       : {},
                        sessions      : {},
                        readers       : [],
                        followed      : []
                    },
                    pub : {
                        tid        : tid,
                        name       : "CN8000",
                        type       : "devcie",
                        part_id    : "0",
                        profile    : "",
                        foreignkey : "",
                        online     : false,
                        time       : null,
                        sessions   : {}
                    }
                };  // end of things record
            }
            this.loaded = true;
            console.log("[smooth db loader] finished");
        }.bind(this)

    ], callback);
    
};

TC_DB.prototype.do_smooth_load = function (callback) {
    if (this.smooth_load.list.length === 0) {
        // load finished
        console.log("[do_smooth_load] finished");
        console.log("[do_smooth_load] time cost: ",
                    new Date() - this.smooth_load.start_at);
        console.log("things : ", this.cache.data.things.length);
        console.log("users  : ", this.cache.data.users.length);
        console.log("xref   : ", this.cache.data.xref.length);
        console.log("parts  : ", this.cache.data.parts.length);
        this.do_smooth_update_cache(callback);
        return;
    }
    var table    = this.smooth_load.list[0].table;
    var member   = this.smooth_load.list[0].member;
    var key_expr = this.smooth_load.list[0].key;
    var step     = this.smooth_load.step;
    var sql;
    if (this.smooth_load.key === null) {
        // start new table
        console.log("[do_smooth_load] start new table", table);
        if (this.config.type === "mssql") {
            sql = "select top " + step.toString() + " " + key_expr + " as k, * "
                + " from " + table
                + " order by " + key_expr;
        } else if (this.config.type === "mysql") {
            sql = "select " + key_expr + " as k, " + table + ".* "
                + " from " + table
                + " order by k limit " + step.toString();
        }
        this.query(sql, [], function (err, recordsets) {
            if (err) {
                console.log("[do_smooth_load]", err);
                callback(err);
            } else {
                this.cache.data[member] =
                    this.cache.data[member].concat(recordsets[0]);
                var len = recordsets[0].length;
                this.smooth_load.counter = this.smooth_load.counter + len;
                if (len === step) {
                    this.smooth_load.key = recordsets[0][len - 1].k;
                    // console.log("[do_smooth_load] next 100");
                } else {
                    // this table finished
                    console.log("[do_smooth_load] table finished",
                                table, this.smooth_load.counter);
                    this.smooth_load.list.shift();
                    this.smooth_load.key = null;
                    this.smooth_load.counter = 0;
                }
                this.do_smooth_load(callback);
            }
        }.bind(this));
    } else {
        // continue last table;
        if (this.config.type === "mssql") {
            sql = "select top " + step.toString() + " " + key_expr + " as k, * "
                + " from " + table
                + " where " + key_expr + " > '" + this.smooth_load.key + "'"
                + " order by " + key_expr;
        } else if (this.config.type === "mysql") {
            sql = "select " + key_expr + " as k, * "
                + " from " + table
                + " where " + key_expr + " > '" + this.smooth_load.key + "'"
                + " order by " + key_expr + " limit " + step.toString();
        }
        this.query(sql, [], function (err, recordsets) {
            if (err) {
                console.log("[do_smooth_load]", err);
                callback(err);
            } else {
                this.cache.data[member] =
                    this.cache.data[member].concat(recordsets[0]);
                var len = recordsets[0].length;
                this.smooth_load.counter = this.smooth_load.counter + len;
                if (len === step) {
                    // console.log("[do_smooth_load] next 100", table);
                    this.smooth_load.key = recordsets[0][len - 1].k;
                } else {
                    // this table finished
                    console.log("[do_smooth_load] table finished",
                                table, this.smooth_load.counter);
                    this.smooth_load.list.shift();
                    this.smooth_load.key = null;
                    this.smooth_load.counter = 0;
                }
                this.do_smooth_load(callback);
            }
        }.bind(this));
    }
};

TC_DB.prototype.smooth_load_data_to_runtime = function (callback) {
    console.log(new Date());
    var key;
    for (key in this.cache.data) {
        if (this.cache.data.hasOwnProperty(key)) {
            this.cache.data_last[key] = this.cache.data[key];
        }
    }
    for (key in this.cache.keys) {
        if (this.cache.keys.hasOwnProperty(key)) {
            this.cache.keys_last[key] = this.cache.keys[key];
        }
    }
    this.cache.data.things = [];
    this.cache.data.users  = [];
    this.cache.data.xref   = [];
    this.cache.data.parts  = [];

    this.smooth_load = {
        key     : null,
        step    : 200,
        counter : 0,
        start_at: new Date()
    };
    if (this.config.type === "mssql") {
	    this.smooth_load.list =
	        [ { table : "_things",      member: "things", key : "tid" },
	          { table : "_users",       member: "users",  key : "tid" },
	          { table : "_things_xref", member: "xref",   key : "owner_tid + tid" },
	          { table : "_parts",       member: "parts",  key : "part_id" }
	        ];
    } else if (this.config.type === "mysql") {
	    this.smooth_load.list =
	        [ { table : "_things",      member: "things", key : "tid" },
	          { table : "_users",       member: "users",  key : "tid" },
	          { table : "_things_xref", member: "xref",   key : "concat (owner_tid, tid)" },
	          { table : "_parts",       member: "parts",  key : "part_id" }
	        ];
    }
    this.do_smooth_load(callback);
};
TC_DB.prototype.smooth_load_clean = function (callback) {
    var sql = "";
    sql = sql + "drop table _things;";
    sql = sql + "drop table _things_xref;";
    sql = sql + "drop table _parts;";
    sql = sql + "drop table _users;";
    this.query(sql, [], function (err, recordsets) {
        callback();
    }.bind(this));
};
TC_DB.prototype.smooth_load_prepare = function (callback) {
    var sql = "";
    if (this.config.type === "mssql") {
	    sql = sql + "drop table _things;";
	    sql = sql + "select * into _things from things where in_use > 0 order by tid ;";
	    sql = sql + "drop table _things_xref;";
	    sql = sql + "select * into _things_xref from things_xref where owner_tid in (select tid from things where in_use > 0) and tid in (select tid from things where in_use > 0) order by owner_tid + tid;";
	    sql = sql + "drop table _parts;";
	    sql = sql + "select * into _parts from parts order by part_id;";
	    sql = sql + "drop table _users ;";
	    sql = sql + "select * into _users from users where tid in (select tid from things where in_use > 0);";
    } else if (this.config.type === "mysql") {
	    sql = sql + "drop table if exists _things;";
	    sql = sql + "create table _things (select * from things where in_use > 0 order by tid);";
	    sql = sql + "drop table if exists _things_xref;";
	    sql = sql + "create table _things_xref (select * from things_xref where owner_tid in (select tid from things where in_use > 0) and tid in (select tid from things where in_use > 0) order by owner_tid + tid);";
	    sql = sql + "drop table if exists _parts;";
	    sql = sql + "create table _parts (select * from parts order by part_id);";
	    sql = sql + "drop table if exists _users ;";
	    sql = sql + "create table _users (select * from users where tid in (select tid from things where in_use > 0));";
    } else {
	    callback({error: "unknow db type"});
    }
    this.query(sql, [], function (err, recordsets) {
        callback();
    }.bind(this));
};
TC_DB.prototype.load_data_to_runtime = function (callback) {
    if (this.smooth_loading) {
        callback({error : "smooth load is running" });
        return;
    }
    this.smooth_loading = true;
    this.smooth_load_start_time = new Date();
    var sql = "select max(mid) + 1 as mid from message;";
    this.query(sql, [], function (err, recordsets) {
        if (err) {
            console.log("[error update things runtime]", err);
            callback(err);
            this.smooth_loading = false;
        } else {
            if (!this.server.runtime_ready) {
                var mid = recordsets[0][0].mid;
                if (!this.server.runtime.system.mid) {
                    this.server.runtime.system.mid = mid | 0;
                }
            }
            this.smooth_load_prepare(function () {
                this.smooth_load_data_to_runtime(function (err) {
                    this.smooth_load_clean(function () {
                        callback(err);
                        this.smooth_loading = false;
                        console.log("[smooth load] cost time =",
                                    new Date() - this.smooth_load_start_time);
                    }.bind(this));
                }.bind(this));
            }.bind(this));
        }
    }.bind(this));
    return;
};


TC_DB.prototype.xref_add = function (owner_tid, tid) {
    // console.log("[xref add]", owner_tid, tid);
    var things = this.server.runtime.things;
    var owner  = things[owner_tid];
    var target = things[tid];
    if (!owner || !target) { return; }
    var i;
    i = target.priv.readers.indexOf(owner.pub.tid);
    if (i < 0) {
        target.priv.readers.push(owner.pub.tid);
    }
    i = owner.priv.followed.indexOf(target.pub.tid);
    if (i < 0) {
        owner.priv.followed.push(target.pub.tid);
        if (owner.pub.online) {
            var sessions = this.server.get_session_by_tid(owner_tid);
            for (i = 0; i < sessions.length; i++) {
                this.server.post_status_of_followed_to_session(sessions[i].sid,
                                                               tid);
            }
        }
    }
};
TC_DB.prototype.xref_del = function (owner_tid, tid) {
    var things = this.server.runtime.things;
    var owner  = things[owner_tid];
    var target = things[tid];
    if (!owner || !target) { return; }
    var i;
    if (owner.pub.online) {
        var sessions = this.server.get_session_by_tid(owner_tid);
        for (i = 0; i < sessions.length; i++) {
            this.server.post_status_of_followed_to_session(sessions[i].sid,
                                                           tid, "del");
        }
    }
    i = owner.priv.followed.indexOf(target.pub.tid);
    if (i >= 0) {
        owner.priv.followed.splice(i, 1);
    }
    i = target.priv.readers.indexOf(owner.pub.tid);
    if (i >= 0) {
        target.priv.readers.splice(i, 1);
    }
};
TC_DB.prototype.sync_add = function (child_tid, parent_tid) {
    // console.log("[sync add]:", child_tid, parent_tid);
    var things = this.server.runtime.things;
    var child  = things[child_tid];
    var parent = things[parent_tid];
    if (!child || !parent) { return; }
    if (parent.pub.type !== "device") {
        console.log("[err] Only device can be sync with. parent.type=",
                   parent.pub.type);
        return;
    }
    var i;
    i = parent.priv.sync_by.indexOf(child_tid);
    if (i < 0) {
        parent.priv.sync_by.push(child_tid);
    }
    child.priv.sync_with = parent_tid;
    if (parent.pub.online) {
        var session = this.server.get_session_by_tid(parent.pub.tid)[0];
        var addr    = session.pub.addr + '.' + child.pub.foreignkey;
        this.server.session_new(child, {
            login_type : 'dev',
            sid        : "",
            tid        : child.pub.tid,
            addr       : addr,
            login_data : {
                remoteAddress : session.pub.raddr,
                remotePort    : session.pub.rport,
                localAddress  : session.pub.laddr,
                localPort     : session.pub.lport
            }
        });
        this.server.post_full_status_to_readers(child_tid);
        this.server.node.request(addr, "/v", function (res) {
            if (res.code !== 200) {
                console.log("[tc.db.js::sync_add] resync success.");
            } else {
                console.log("[tc.db.js::sync_add] resync fail.");
            }
        });

    }
};
TC_DB.prototype.sync_del = function (child_tid, parent_tid) {
    var things = this.server.runtime.things;
    var child  = things[child_tid];
    var parent = things[parent_tid];
    if (!child || !parent) { return; }
    var i;
    var sessions = this.server.get_session_by_tid(child_tid);
    if (sessions.length > 0) {
        for (i = 0; i < sessions.length; i++) {
            this.server.session_del(sessions[i].sid);
        }
        child.pub.online = false;
        console.log("[sync del] post status to readers:", child_tid);
        this.server.post_full_status_to_readers(child_tid);
    }
    i = parent.priv.sync_by.indexOf(child_tid);
    if (i >= 0) {
        parent.priv.sync_by.splice(i, 1);
    }
    child.priv.sync_with = "";
};
TC_DB.prototype.things_enable  = function () {};
TC_DB.prototype.things_disable = function () {};
TC_DB.prototype.things_new     = function (t) {
    // console.log("[things new]", t.tid);
    if (this.server.runtime.things[t.tid]) { return; }
    var pub_profile, priv_profile;
    try {
        priv_profile = JSON.parse(t.private_profile);
    } catch (error) {
        priv_profile = t.private_profile;
    }
    try {
        pub_profile = JSON.parse(t.public_profile);
    } catch (error) {
        pub_profile = t.public_profile;
    }
    this.server.runtime.things[t.tid] = {
        priv : {
            addr_type     : t.addr_type,
            sync_with     : t.sync_with,
            sync_by       : [],
            seqnum        : 1,
            addr_template : t.addr_template,
            profile       : priv_profile,
            readers       : [],
            followed      : [],
            unreads       : {},
            sessions      : {}
        },
        pub: {
            tid            : t.tid,
            name           : t.name,
            type           : t.type,
            part_id        : t.part_id,
            profile        : pub_profile,
            foreignkey     : t.foreignkey,
            online         : false,
            sessions       : {}
        }
    };
};

TC_DB.prototype.things_del     = function (t) {
    var thing = this.server.runtime.things[t.tid];
    if (!thing) { return; }
    if (thing.pub.online) {
        var key;
        for (key in thing.pub.sessions) {
            if (thing.pub.sessions.hasOwnProperty(key)) {
                var session = thing.pub.sessions[key];
                this.server.session_del(session.sid);
            }
        }
    }
    delete this.server.runtime.things[t.tid];
};
TC_DB.prototype.things_update  = function (t) {
    var thing = this.server.runtime.things[t.tid];
    if (!thing) { return; }
    var pub_profile, priv_profile;
    try {
        priv_profile = JSON.parse(t.private_profile);
    } catch (error) {
        priv_profile = t.private_profile;
    }
    try {
        pub_profile = JSON.parse(t.public_profile);
    } catch (error) {
        pub_profile = t.public_profile;
    }
    thing.priv.addr_type     = t.addr_type;
    thing.priv.addr_template = t.addr_template;
    thing.priv.profile       = priv_profile;
    thing.pub.name           = t.name;
    thing.pub.type           = t.type;
    thing.pub.part_id        = t.part_id;
    thing.pub.profile        = pub_profile;
    thing.pub.foreignkey     = t.foreignkey;

    this.server.post_full_status_to_readers(t.tid);
};

TC_DB.prototype.update_step_1_xref_del = function () {
    // console.log("[update step 1 xref del]");
    var xref = this.cache.data_last.xref;
    var i = 2000;
    while ((i > 0) && (this.cache.diff.xref.removed.length > 0)) {
        var key = this.cache.diff.xref.removed.shift();
        this.xref_del(xref[key].owner_tid, xref[key].tid);
        i--;
    }
    return this.cache.diff.xref.removed.length;
};
TC_DB.prototype.update_step_2_sync_del       = function () {
    // console.log("[update step 2 sync del]");
    var keys = this.cache.keys_last.sync;
    var i = 1000;
    while ((i > 0) && (this.cache.diff.sync.removed.length > 0)) {
        var key = this.cache.diff.sync.removed.shift();
        var a = keys[key].split("|");
        var child  = a[0];
        var parent = a[1];
        this.sync_del(child, parent);
        i--;
    }
    return this.cache.diff.sync.removed.length;
};
TC_DB.prototype.update_step_3_things_disable = function () {};
TC_DB.prototype.update_step_4_things_del     = function () {
    // console.log("[update step 4 things del]");
    var things = this.cache.data_last.things;
    var i = 1000;
    while ((i > 0) && (this.cache.diff.things.removed.length > 0)) {
        var key = this.cache.diff.things.removed.shift();
        this.things_del(things[key]);
        i--;
    }
    return this.cache.diff.things.removed.length;
};
TC_DB.prototype.update_step_5_general_update  = function () {
};
TC_DB.prototype.update_step_6_things_new     = function () {
    // console.log("[update step 6 things new]");
    var things = this.cache.data.things;
    var i = 200;
    while ((i > 0) && (this.cache.diff.things.added.length > 0)) {
        var key = this.cache.diff.things.added.shift();
        this.things_new(things[key]);
        i--;
    }
    return this.cache.diff.things.added.length;
};
TC_DB.prototype.update_step_7_things_enable  = function () {};
TC_DB.prototype.update_step_8_sync_add       = function () {
    // console.log("[sync add]");
    var keys = this.cache.keys.sync;
    var i = 20;
    while ((i > 0) && (this.cache.diff.sync.added.length > 0)) {
        var key = this.cache.diff.sync.added.shift();
        var a = keys[key].split("|");
        var child  = a[0];
        var parent = a[1];
        this.sync_add(child, parent);
        i--;
    }
    return this.cache.diff.sync.added.length;
};
TC_DB.prototype.update_step_9_xref_add       = function () {
    // console.log("[update step 9 xref add]");
    var xref = this.cache.data.xref;
    var i = 50;
    while ((i > 0) && (this.cache.diff.xref.added.length > 0)) {
        var key = this.cache.diff.xref.added.shift();
        this.xref_add(xref[key].owner_tid, xref[key].tid);
        i--;
    }
    return this.cache.diff.xref.added.length;
};

TC_DB.prototype.update_step_10_update_things = function () {
    // console.log("[update step 10 update things]");
    // update things
    var cur  = this.cache.data.things;
    var last = this.cache.data_last.things;
    var i = 1000;
    while ((i > 0) && (this.cache.diff.things.unchanged.length > 0)) {
        var key = this.cache.diff.things.unchanged.shift();
        var t1 = cur[key.cur];
        var t2 = last[key.last];
        var s1 = JSON.stringify(t1);
        var s2 = JSON.stringify(t2);
        if (s1 !== s2) {
            this.things_update(t1);
        }
        i--;
    }
    return this.cache.diff.things.unchanged.length;
};

TC_DB.prototype.update_step_11_update_parts = function () {
    // console.log("[update step 11 update parts]");
    var db      = this.cache.data.parts;
    var runtime = {};
    var i;
    for (i = 0; i < db.length; i++) {
        var part = db[i];
        var s = part.define;
        try {
            part.define = JSON.parse(s);
        } catch (error) {
            console.log("error : ", error);
            console.log("error parse part define : ", part);
            part.define = {error: "error parse json string"};
        }
        runtime[part.part_id] = part;
    }
    this.server.runtime.parts = runtime;
};

TC_DB.prototype.update_step_12_update_users = function () {
    // console.log("[update step 12 update users]");
    var db      = this.cache.data.users;
    var runtime = {};
    var i;
    for (i = 0; i < db.length; i++) {
        var user = db[i];
        runtime[user.tid] = user;
    }
    this.server.runtime.users = runtime;
};

function TC_DB_MSSQL(server, config) {
    if (!config.options) {
        config.options = {};
    }
    config.options.useUTC = false;
    TC_DB.call(this, server, config);
    this.mssql = require("mssql");
    this.connection = null;
}
util.inherits(TC_DB_MSSQL, TC_DB);

TC_DB_MSSQL.prototype.connect = function (callback) {
    console.log("[db config]", this.config);
    this.connection = new this.mssql.Connection(this.config, function (err) {
        callback(err);
    }.bind(this));
    this.connection.on('error', function (err) {
        console.log(err);
        this.server.make_db_connection();
    }.bind(this));
};
TC_DB_MSSQL.prototype.query = function (sql, param, callback) {
    var request = new this.mssql.Request(this.connection);
    // or: var request = connection.request();
    // console.log("SQL = ", sql);
    request.multiple = true;
    request.on('error', function (err) {
        console.log(err);
        callback(err);
    }.bind(this));
    request.query(sql, function (err, recordsets) {
        callback(err, recordsets);
    }.bind(this));
};
TC_DB_MSSQL.prototype.get_message_of = function (param, callback) {
    console.log("get message of ", param);
    var t = this.server.runtime.things[param.of_tid];
    var type = t.pub.type;
    var sql = '';
    if (type !== "group") {
        if (param.mode === 'last') {
            sql = sql + "select top (@cnt) mailbox.*, [message].*, things_from.type as from_things_type, things_to.type as to_things_type  from mailbox ";
            sql = sql + "    left join [message]            on mailbox.mid  = [message].mid";
            sql = sql + "    left join things things_from on [message].[from] = things_from.tid";
            sql = sql + "    left join things things_to   on [message].[to]   = things_to.tid";
            sql = sql + "    where owner_tid = @my_tid and code != 1 and code != 2 and ([message].[from] = @of_tid or [message].[to] = @of_tid)";
            sql = sql + "    order by [message].mid desc;";
        } else if (param.mode === 'before') {
            sql = sql + "select  top (@cnt) mailbox.*, [message].*, things_from.type as from_things_type, things_to.type as to_things_type  from mailbox ";
            sql = sql + "    left join [message]";
            sql = sql + "    on mailbox.mid = [message].mid";
            sql = sql + "    left join things things_from";
            sql = sql + "    on [message].[from] = things_from.tid";
            sql = sql + "    left join things things_to";
            sql = sql + "    on [message].[to] = things_to.tid";
            sql = sql + "    where owner_tid = @my_tid and code != 1 and code != 2 and mailbox.mid < @mid and ([message].[from] = @of_tid or [message].[to] = @of_tid)";
            sql = sql + "    order by [message].mid desc;";

        } else if (param.mode === 'first') {
            sql = sql + "select  mailbox.*, [message].*, things_from.type as from_things_type, things_to.type as to_things_type  from mailbox ";
            sql = sql + "    left join [message]";
            sql = sql + "    on mailbox.mid = [message].mid";
            sql = sql + "    left join things things_from";
            sql = sql + "    on [message].[from] = things_from.tid";
            sql = sql + "    left join things things_to";
            sql = sql + "    on [message].[to] = things_to.tid";
            sql = sql + "    where owner_tid = @my_tid and and code != 1 and code != 2([message].[from] = @of_tid or [message].[to] = @of_tid)";
            sql = sql + "    order by [message].mid;";
        } else if (param.mode === 'after') {
            sql = sql + "select  top (@cnt) mailbox.*, [message].*, things_from.type as from_things_type, things_to.type as to_things_type  from mailbox ";
            sql = sql + "    left join [message]";
            sql = sql + "    on mailbox.mid = [message].mid";
            sql = sql + "    left join things things_from";
            sql = sql + "    on [message].[from] = things_from.tid";
            sql = sql + "    left join things things_to";
            sql = sql + "    on [message].[to] = things_to.tid";
            sql = sql + "    where owner_tid = @my_tid and and code != 1 and code != 2 mailbox.mid > @mid and ([message].[from] = @of_tid or [message].[to] = @of_tid)";
            sql = sql + "    order by [message].mid;";

        }
    } else {
        if (param.mode === 'last') {
            sql = sql + "select  mailbox.*, [message].*, things_from.type as from_things_type, things_to.type as to_things_type  from mailbox ";
            sql = sql + "    left join [message]";
            sql = sql + "    on mailbox.mid = [message].mid";
            sql = sql + "    left join things things_from";
            sql = sql + "    on [message].[from] = things_from.tid";
            sql = sql + "    left join things things_to";
            sql = sql + "    on [message].[to] = things_to.tid";
            sql = sql + "    where owner_tid = @my_tid and and code != 1 and code != 2 [message].[to] = @of_tid";
            sql = sql + "    order by [message].mid desc";

        } else if (param.mode === 'before') {
            sql = sql + "select  top (@cnt) mailbox.*, [message].*, things_from.type as from_things_type, things_to.type as to_things_type  from mailbox ";
            sql = sql + "    left join [message]";
            sql = sql + "    on mailbox.mid = [message].mid";
            sql = sql + "    left join things things_from";
            sql = sql + "    on [message].[from] = things_from.tid";
            sql = sql + "    left join things things_to";
            sql = sql + "    on [message].[to] = things_to.tid";
            sql = sql + "    where owner_tid = @my_tid and code != 1 and code != 2 and mailbox.mid < @mid and [message].[to] = @of_tid";
            sql = sql + "    order by [message].mid desc;";

        } else if (param.mode === 'first') {
            sql = sql + "select  top (@cnt) mailbox.*, [message].*, things_from.type as from_things_type, things_to.type as to_things_type  from mailbox ";
            sql = sql + "    left join [message]";
            sql = sql + "    on mailbox.mid = [message].mid";
            sql = sql + "    left join things things_from";
            sql = sql + "    on [message].[from] = things_from.tid";
            sql = sql + "    left join things things_to";
            sql = sql + "    on [message].[to] = things_to.tid";
            sql = sql + "    where owner_tid = @my_tid and code != 1 and code != 2 and [message].[to] = @of_tid";
            sql = sql + "    order by [message].mid;";
        } else if (param.mode === 'after') {
            sql = sql + "select  top (@cnt) mailbox.*, [message].*, things_from.type as from_things_type, things_to.type as to_things_type  from mailbox ";
            sql = sql + "    left join [message]";
            sql = sql + "    on mailbox.mid = [message].mid";
            sql = sql + "    left join things things_from";
            sql = sql + "    on [message].[from] = things_from.tid";
            sql = sql + "    left join things things_to";
            sql = sql + "    on [message].[to] = things_to.tid";
            sql = sql + "    where owner_tid = @my_tid and code != 1 and code != 2 and mailbox.mid > @mid and [message].[to] = @of_tid";
            sql = sql + "    order by [message].mid;";
        }
    }
    var ps = new this.mssql.PreparedStatement(this.connection);
    ps.multiple = true;
    ps.input("my_tid", this.mssql.VarChar(32));
    ps.input("of_tid", this.mssql.VarChar(32));
    ps.input("mid",    this.mssql.BigInt);
    ps.input("cnt",    this.mssql.Int);

    ps.prepare(sql, function (err) {
        if (err) {
            console.log(sql);
            console.log("[prepare error]", err);
            ps.unprepare();
        } else {
            ps.execute(param, function (err, recordsets) {
                if (err) {
                    console.log(err);
                }
                if (typeof callback === 'function') {
                    var result = recordsets[0];
                    var i;
                    for (i = 0; i < result.length; i++) {
                        result[i].mid = result[i].mid[0];
                        var time = new Date(result[i].time);
                        result[i].time = time.format("yyyy-MM-dd hh:mm:ss");
                    }
                    callback(err, recordsets);
                }
                ps.unprepare();
            });
        }
    });

};
TC_DB_MSSQL.prototype.save_new_message = function (type, message, callback) {
    console.log("[mssql] save new message.");
    if (!this.server.db_connected) {
        console.log("database connection lost. message =", message);
        if (callback) {
            callback({error : "database connection lost"});
        }
        return;
    }
    var ps = new this.mssql.PreparedStatement(this.connection);
    ps.input("mid",    this.mssql.BigInt);
    ps.input("time",   this.mssql.VarChar(30));
    ps.input("from",   this.mssql.VarChar(32));
    ps.input("to",     this.mssql.VarChar(32));
    ps.input("type",   this.mssql.VarChar(50));
    ps.input("code",   this.mssql.Int);
    ps.input("format", this.mssql.VarChar(10));
    ps.input("body",   this.mssql.NText);
    var sql = "insert into message values ";
    sql = sql + "(@mid, 0, @time, @from, @to, @type, @code, @format, @body);\n";
    if (type === "im") {
        sql = sql + "insert into mailbox (owner_tid, mid) values (@from, @mid);";
        sql = sql + "insert into mailbox (owner_tid, mid) values (@to, @mid);";
    } else if (type === "im_to_group") {
        sql = sql + "insert into mailbox (owner_tid, mid) ";
        sql = sql + "  select owner_tid, @mid from things_xref where tid = @to;";
    } else if (type === "event") {
        sql = sql + "insert into mailbox (owner_tid, mid) ";
        sql = sql + "select owner_tid, @mid from things_xref where tid = @from;";
    }
    
    ps.prepare(sql, function (err) {
        if (err) {
            console.log(err);
            ps.unprepare();
        } else {
            var msg = {};
            msg.mid    = message.mid;
            msg.time   = message.time;
            msg.from   = message.from;
            msg.to     = message.to;
            msg.type   = message.type;
            msg.code   = message.code;
            msg.format = message.format;
            msg.body   = message.body_text;
            ps.execute(msg, function (err, recordsets) {
                if (err) {
                    console.log(err);
                }
                if (callback) {
                    callback(err);
                }
                ps.unprepare();
            });
        }
    });
};

function TC_DB_MYSQL(server, config) {
    if (config.db && config.db.server) {
        config.db.host = config.db.server;
    }
    TC_DB.call(this, server, config);
    this.config.multipleStatements = true;
    this.mysql  = require("mysql");
    this.connection = null;
}
util.inherits(TC_DB_MYSQL, TC_DB);

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
TC_DB_MYSQL.prototype.get_message_of = function (param, callback) {
    console.log("get message of ", param);
    var t = this.server.runtime.things[param.of_tid];
    var type = t.pub.type;
    var sql = '';
    if (type !== "group") {
        if (param.mode === 'last') {
            sql = sql + "select mailbox.*, [message].*, things_from.type as from_things_type, things_to.type as to_things_type  from mailbox ";
            sql = sql + "    left join [message]            on mailbox.mid  = [message].mid";
            sql = sql + "    left join things things_from on [message].[from] = things_from.tid";
            sql = sql + "    left join things things_to   on [message].[to]   = things_to.tid";
            sql = sql + "    where owner_tid = @my_tid and code != 1 and code != 2 and ([message].[from] = @of_tid or [message].[to] = @of_tid)";
            sql = sql + "    order by [message].mid desc limit @cnt;";
        } else if (param.mode === 'before') {
            sql = sql + "select  mailbox.*, [message].*, things_from.type as from_things_type, things_to.type as to_things_type  from mailbox ";
            sql = sql + "    left join [message]";
            sql = sql + "    on mailbox.mid = [message].mid";
            sql = sql + "    left join things things_from";
            sql = sql + "    on [message].[from] = things_from.tid";
            sql = sql + "    left join things things_to";
            sql = sql + "    on [message].[to] = things_to.tid";
            sql = sql + "    where owner_tid = @my_tid and code != 1 and code != 2 and mailbox.mid < @mid and ([message].[from] = @of_tid or [message].[to] = @of_tid)";
            sql = sql + "    order by [message].mid desc  limit @cnt;";

        } else if (param.mode === 'first') {
            sql = sql + "select  mailbox.*, [message].*, things_from.type as from_things_type, things_to.type as to_things_type  from mailbox ";
            sql = sql + "    left join [message]";
            sql = sql + "    on mailbox.mid = [message].mid";
            sql = sql + "    left join things things_from";
            sql = sql + "    on [message].[from] = things_from.tid";
            sql = sql + "    left join things things_to";
            sql = sql + "    on [message].[to] = things_to.tid";
            sql = sql + "    where owner_tid = @my_tid and code != 1 and code != 2 and ([message].[from] = @of_tid or [message].[to] = @of_tid)";
            sql = sql + "    order by [message].mid;";
        } else if (param.mode === 'after') {
            sql = sql + "select  mailbox.*, [message].*, things_from.type as from_things_type, things_to.type as to_things_type  from mailbox ";
            sql = sql + "    left join [message]";
            sql = sql + "    on mailbox.mid = [message].mid";
            sql = sql + "    left join things things_from";
            sql = sql + "    on [message].[from] = things_from.tid";
            sql = sql + "    left join things things_to";
            sql = sql + "    on [message].[to] = things_to.tid";
            sql = sql + "    where owner_tid = @my_tid and code != 1 and code != 2 and mailbox.mid > @mid and ([message].[from] = @of_tid or [message].[to] = @of_tid)";
            sql = sql + "    order by [message].mid  limit @cnt;";
        }
    } else {
        if (param.mode === 'last') {
            sql = sql + "select  mailbox.*, [message].*, things_from.type as from_things_type, things_to.type as to_things_type  from mailbox ";
            sql = sql + "    left join [message]";
            sql = sql + "    on mailbox.mid = [message].mid";
            sql = sql + "    left join things things_from";
            sql = sql + "    on [message].[from] = things_from.tid";
            sql = sql + "    left join things things_to";
            sql = sql + "    on [message].[to] = things_to.tid";
            sql = sql + "    where owner_tid = @my_tid and code != 1 and code != 2 and [message].[to] = @of_tid";
            sql = sql + "    order by [message].mid desc";

        } else if (param.mode === 'before') {
            sql = sql + "select  mailbox.*, [message].*, things_from.type as from_things_type, things_to.type as to_things_type  from mailbox ";
            sql = sql + "    left join [message]";
            sql = sql + "    on mailbox.mid = [message].mid";
            sql = sql + "    left join things things_from";
            sql = sql + "    on [message].[from] = things_from.tid";
            sql = sql + "    left join things things_to";
            sql = sql + "    on [message].[to] = things_to.tid";
            sql = sql + "    where owner_tid = @my_tid and code != 1 and code != 2 and mailbox.mid < @mid and [message].[to] = @of_tid";
            sql = sql + "    order by [message].mid desc limit @cnt;";

        } else if (param.mode === 'first') {
            sql = sql + "select  mailbox.*, [message].*, things_from.type as from_things_type, things_to.type as to_things_type  from mailbox ";
            sql = sql + "    left join [message]";
            sql = sql + "    on mailbox.mid = [message].mid";
            sql = sql + "    left join things things_from";
            sql = sql + "    on [message].[from] = things_from.tid";
            sql = sql + "    left join things things_to";
            sql = sql + "    on [message].[to] = things_to.tid";
            sql = sql + "    where owner_tid = @my_tid and code != 1 and code != 2 and [message].[to] = @of_tid";
            sql = sql + "    order by [message].mid  limit @cnt;";
        } else if (param.mode === 'after') {
            sql = sql + "select  mailbox.*, [message].*, things_from.type as from_things_type, things_to.type as to_things_type  from mailbox ";
            sql = sql + "    left join [message]";
            sql = sql + "    on mailbox.mid = [message].mid";
            sql = sql + "    left join things things_from";
            sql = sql + "    on [message].[from] = things_from.tid";
            sql = sql + "    left join things things_to";
            sql = sql + "    on [message].[to] = things_to.tid";
            sql = sql + "    where owner_tid = @my_tid and code != 1 and code != 2 and mailbox.mid > @mid and [message].[to] = @of_tid";
            sql = sql + "    order by [message].mid  limit @cnt ;";
        }
    }
    var p;
    for (p in param) {
        if (param.hasOwnProperty(p)) {
            var regexp = new RegExp("@" + p, "g");
            if (typeof param[p] === 'string') {
                sql = sql.replace(regexp, "'" + param[p] + "'");
            } else {
                sql = sql.replace(regexp, param[p]);
            }
        }
    }
    sql = sql.replace(/[\[\]]/g, "");
    this.query(sql, [], function (err, recordsets) {
        if (typeof callback === 'function') {
            var result = recordsets[0];
            var i;
            for (i = 0; i < result.length; i++) {
                var time = new Date(result[i].time);
                result[i].time = time.format("yyyy-MM-dd hh:mm:ss");
            }
            callback(err, recordsets);
        }
    });
};
TC_DB_MYSQL.prototype.save_new_message = function (type, message, callback) {

// insert into message values (undefined, 0, '2014-11-28 12:30:32', 'USER-FIQ-S16-JO', 'DEMO-3O1-9D6-AL', 'im', 0, 'cmd', 'open');
// insert into mailbox (owner_tid, mid) values ('USER-FIQ-S16-JO', undefined);
// insert into mailbox (owner_tid, mid) values ('DEMO-3O1-9D6-AL', undefined);

    console.log("[mysql] save new message.", message);
    var sql = "insert into message values ";
    sql = sql + "(0, @mid, @time, @from, @to, @type, @code, @format, @body);\n";
    if (type === "im") {
        sql = sql + "insert into mailbox (owner_tid, mid) values (@from, @mid);";
        sql = sql + "insert into mailbox (owner_tid, mid) values (@to, @mid);";
    } else if (type === "im_to_group") {
        sql = sql + "insert into mailbox (owner_tid, mid) ";
        sql = sql + "  select owner_tid, @mid from things_xref where tid = @to;";
    } else if (type === "event") {
        sql = sql + "insert into mailbox (owner_tid, mid) ";
        sql = sql + "select owner_tid, @mid from things_xref where tid = @from;";
    }
    var msg = {};
    msg.mid    = message.mid;
    msg.time   = message.time;
    msg.from   = message.from;
    msg.to     = message.to;
    msg.type   = message.type;
    msg.code   = message.code;
    msg.format = message.format;
    msg.body   = message.body_text;
    var p;
    for (p in msg) {
        if (msg.hasOwnProperty(p)) {
            var regexp = new RegExp("@" + p, "g");
            if (typeof msg[p] === 'string') {
                sql = sql.replace(regexp, "'" + msg[p] + "'");
            } else {
                sql = sql.replace(regexp, msg[p]);
            }
        }
    }
    sql = sql.replace(/[\[\]]/g, "");

    this.query(sql, [], function (err, recordsets) {
        if (callback) {
            callback(err, recordsets);
        }
    });
};

module.exports = function (server, config) {
    if (config.type === "mssql") {
        return new TC_DB_MSSQL(server, config);
    } else if (config.type === "mysql") {
        return new TC_DB_MYSQL(server, config);
    }
};
