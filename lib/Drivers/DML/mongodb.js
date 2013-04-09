var mongodb = require("mongodb");

exports.Driver = Driver;

function Driver(config, connection, opts) {
	this.config = config || {};
	this.opts   = opts || {};

	this.db     = null;
	if (connection) {
		this.db = connection;
	} else {
		this.db = new mongodb.Db(
			config.database || "test",
			new mongodb.Server(config.host || "localhost", config.port || 27017, {
				auto_reconnect : true,
				databaseName   : config.database
			}), {
				w : 1
			}
		);
	}
}

Driver.prototype.connect = function (cb) {
	this.db.open(cb);
};

Driver.prototype.on = function (ev, cb) {
	if (ev == "error") {
		this.db.on("error", cb);
	}
	return this;
};

Driver.prototype.find = function (fields, table, conditions, opts, cb) {
	this.db.collection(table, function (err, col) {
		if (err) {
			return cb(err);
		}

		var query_opts = {};

		if (opts.offset) {
			query_opts.skip = opts.offset;
		}
		if (typeof opts.limit == "number") {
			query_opts.limit = opts.limit;
		}
		if (opts.order) {
			query_opts.order = [ [ opts.order[0], opts.order[1] == 'Z' ? 'desc' : 'asc' ] ];
		}

		look_for_ids(conditions);

		return col.find(conditions, fields, query_opts).toArray(cb);
	});

	// if (opts.merge) {
	// 	q.from(opts.merge.from.table, opts.merge.from.field, opts.merge.to.field);
	// 	if (opts.merge.where && Object.keys(opts.merge.where[1]).length) {
	// 		q = q.where(opts.merge.where[0], opts.merge.where[1], conditions);
	// 	}
	// }

	// if (opts.exists) {
	// 	for (var k in opts.exists) {
	// 		q.whereExists(opts.exists[k].table, table, opts.exists[k].link, opts.exists[k].conditions);
	// 	}
	// }
};

Driver.prototype.insert = function (table, data, id_prop, cb) {
	this.db.collection(table, function (err, col) {
		if (err) {
			return cb(err);
		}

		col.insert(data, { safe : true }, function (err, records) {
			if (err) {
				return cb(err);
			}

			return cb(null, { id: records[0]._id });
		});
	});
};

Driver.prototype.update = function (table, changes, conditions, cb) {
	this.db.collection(table, function (err, col) {
		if (err) {
			return cb(err);
		}

		col.update(conditions, { $set: changes }, {
			safe : true
		}, cb);
	});
};

function look_for_ids(obj) {
	for (var k in obj) {
		if (k != "_id") continue;

		obj[k] = str_to_objectid(obj[k]);
	}
}

function str_to_objectid(str) {
	if (Array.isArray(str)) {
		return str.map(str_to_objectid);
	}

	return new mongodb.ObjectID(str);
}
