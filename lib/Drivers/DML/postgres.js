var postgres = require("pg");
var Query    = require("sql-query").Query;

exports.Driver = Driver;

function Driver(config, connection, opts) {
	this.config = config || {};
	this.opts   = opts || {};
	this.query  = new Query("postgresql");

	if (connection) {
		this.db = connection;
	} else if (config.query && config.query.ssl) {
		config.ssl = true;
		this.db = new postgres.Client(config);
	} else {
		this.db = new postgres.Client(config.href || config);
	}

	this.aggregate_functions = [ "ABS", "CEIL", "FLOOR", "ROUND",
	                             "AVG", "MIN", "MAX",
	                             "LOG", "EXP", "POWER",
	                             "ACOS", "ASIN", "ATAN", "COS", "SIN", "TAN",
	                             "RANDOM", "RADIANS", "DEGREES",
	                             "SUM", "COUNT" ];
}

Driver.prototype.sync = function (opts, cb) {
	return require("../DDL/postgres").sync(this, opts, cb);
};

Driver.prototype.drop = function (opts, cb) {
	return require("../DDL/postgres").drop(this, opts, cb);
};

Driver.prototype.ping = function (cb) {
	this.db.query("SELECT * FROM pg_stat_activity LIMIT 1", function () {
		return cb();
	});
	return this;
};

Driver.prototype.on = function (ev, cb) {
	if (ev == "error") {
		this.db.on("error", cb);
	}
	return this;
};

Driver.prototype.connect = function (cb) {
	this.db.connect(cb);
};

Driver.prototype.close = function (cb) {
	this.db.end(cb);
};

Driver.prototype.getQuery = function () {
	return this.query;
};

Driver.prototype.execQuery = function (query, cb) {
	this.db.query(query, handleQuery(cb));
};

Driver.prototype.find = function (fields, table, conditions, opts, cb) {
	var q = this.query.select()
	                  .from(table).select(fields);

	if (opts.offset) {
		q.offset(opts.offset);
	}
	if (typeof opts.limit == "number") {
		q.limit(opts.limit);
	}
	if (opts.order) {
		q.order(opts.order[0], opts.order[1]);
	}

	if (opts.merge) {
		q.from(opts.merge.from.table, opts.merge.from.field, opts.merge.to.field).select(opts.merge.select);
		if (opts.merge.where && Object.keys(opts.merge.where[1]).length) {
			q = q.where(opts.merge.where[0], opts.merge.where[1], conditions);
		} else {
			q = q.where(conditions);
		}
	} else {
		q = q.where(conditions);
	}

	if (opts.exists) {
		for (var k in opts.exists) {
			q.whereExists(opts.exists[k].table, table, opts.exists[k].link, opts.exists[k].conditions);
		}
	}

	q = q.build();

	if (this.opts.debug) {
		require("../../Debug").sql('postgres', q);
	}
	this.db.query(q, handleQuery(cb));
};

Driver.prototype.count = function (table, conditions, opts, cb) {
	var q = this.query.select()
	                  .from(table)
	                  .count(opts.id, 'c');

	if (opts.merge) {
		q.from(opts.merge.from.table, opts.merge.from.field, opts.merge.to.field);
		if (opts.merge.where && Object.keys(opts.merge.where[1]).length) {
			q = q.where(opts.merge.where[0], opts.merge.where[1], conditions);
		} else {
			q = q.where(conditions);
		}
	} else {
		q = q.where(conditions);
	}

	if (opts.exists) {
		for (var k in opts.exists) {
			q.whereExists(opts.exists[k].table, table, opts.exists[k].link, opts.exists[k].conditions);
		}
	}

	q = q.build();

	if (this.opts.debug) {
		require("../../Debug").sql('postgres', q);
	}
	this.db.query(q, handleQuery(cb));
};

Driver.prototype.insert = function (table, data, id_prop, cb) {
	var q = this.query.insert()
	                  .into(table)
	                  .set(data)
	                  .build();

	if (this.opts.debug) {
		require("../../Debug").sql('postgres', q);
	}
	this.db.query(q + " RETURNING *", function (err, result) {
		if (err) {
			return cb(err);
		}
		return cb(null, {
			id: result.rows[0][id_prop] || null
		});
	});
};

Driver.prototype.update = function (table, changes, conditions, cb) {
	var q = this.query.update()
	                  .into(table)
	                  .set(changes)
	                  .where(conditions)
	                  .build();

	if (this.opts.debug) {
		require("../../Debug").sql('postgres', q);
	}
	this.db.query(q, handleQuery(cb));
};

Driver.prototype.remove = function (table, conditions, cb) {
	var q = this.query.remove()
	                  .from(table)
	                  .where(conditions)
	                  .build();

	if (this.opts.debug) {
		require("../../Debug").sql('postgres', q);
	}
	this.db.query(q, handleQuery(cb));
};

Driver.prototype.clear = function (table, cb) {
	var query = "TRUNCATE TABLE " + this.query.escapeId(table);

	if (this.opts.debug) {
		require("../../Debug").sql('postgres', query);
	}
	this.db.query(query, handleQuery(cb));
};

Driver.prototype.valueToProperty = function (value, property) {
	switch (property.type) {
		case "object":
			try {
				return JSON.parse(value);
			} catch (e) {
				return null;
			}
			break;
		default:
			return value;
	}
};

Driver.prototype.propertyToValue = function (value, property) {
	switch (property.type) {
		case "object":
			return JSON.stringify(value);
		default:
			return value;
	}
};

Driver.prototype.infer = function(table, callback) {
  var query = "SELECT column_name, data_type FROM information_schema.columns WHERE table_name = '" + table + "'";
	if (this.opts.debug) {
		require("../../Debug").sql('postgres', query);
	}
	this.db.query(query, handleQuery(function(err, results) {
    if(err) {
      callback(err);
    } else {
      var properties = {}
      if(results.rows) {
        for(var row in results.rows) {
          properties[results.rows[row].column_name] = getClassForType(results.rows[row].data_type);
        }
      }
      callback(err, properties);
    }
  }));
}

function getClassForType(type) {
  switch(type) {
    case "int":
      return Number;
    case "decimal":
      return Number;
    case "character varying":
      return String;
    case "text":
      return String;
    case "boolean":
      return Boolean;
    default:
      throw("Unkown type found during inference: " + type);
  }
}

function handleQuery(cb) {
	return function (err, result) {
		if (err) {
			return cb(err);
		}
		return cb(null, result.rows);
	};
}
