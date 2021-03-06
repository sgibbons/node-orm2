var common = exports;
var path   = require('path');
var ORM    = require('../');

common.ORM = ORM;

common.protocol = function () {
	return process.env.ORM_PROTOCOL;
};

common.isTravis = function() {
	return Boolean(process.env.CI);
};

common.createConnection = function(cb) {
	ORM.connect(this.getConnectionString(), cb);
};

common.getConnectionString = function () {
	var url;

	if (common.isTravis()) {
		switch (this.protocol()) {
			case 'mysql':
				return 'mysql://root@localhost/orm_test';
			case 'postgres':
			case 'redshift':
				return 'postgres://postgres@localhost/orm_test';
			case 'sqlite':
				return 'sqlite://';
			default:
				throw new Error("Unknown protocol");
		}
	} else {
		var config = require("./config")[this.protocol()];

		switch (this.protocol()) {
			case 'mysql':
				return 'mysql://' +
				       (config.user || 'root') +
				       (config.password ? ':' + config.password : '') +
				       '@' + (config.host || 'localhost') +
				       '/' + (config.database || 'orm_test');
			case 'postgres':
				return 'postgres://' +
				       (config.user || 'postgres') +
				       (config.password ? ':' + config.password : '') +
				       '@' + (config.host || 'localhost') +
				       '/' + (config.database || 'orm_test');
			case 'redshift':
				return 'redshift://' +
				       (config.user || 'postgres') +
				       (config.password ? ':' + config.password : '') +
				       '@' + (config.host || 'localhost') +
				       '/' + (config.database || 'orm_test');
			case 'sqlite':
				return 'sqlite://' + (config.pathname || "");
			default:
				throw new Error("Unknown protocol");
		}
	}
	return url;
};

common.getModelProperties = function () {
	return {
		name: { type: "text", defaultValue: "test_default_value" }
	};
};

common.createModelTable = function (table, db, cb) {
	switch (this.protocol()) {
		case "postgres":
		case "redshift":
			db.query("CREATE TEMPORARY TABLE " + table + " (id SERIAL PRIMARY KEY, name VARCHAR(100) NOT NULL)", cb);
			break;
		case "sqlite":
			db.run("DROP TABLE IF EXISTS " + table, function () {
				db.run("CREATE TABLE " + table + " (id INTEGER NOT NULL, name VARCHAR(100) NOT NULL, PRIMARY KEY(id))", cb);
			});
			break;
		default:
			db.query("CREATE TEMPORARY TABLE " + table + " (id BIGINT NOT NULL PRIMARY KEY AUTO_INCREMENT, name VARCHAR(100) NOT NULL)", cb);
			break;
	}
};

common.createModel2Table = function (table, db, cb) {
	switch (this.protocol()) {
		case "postgres":
		case "redshift":
			db.query("CREATE TEMPORARY TABLE " + table + " (id SERIAL PRIMARY KEY, name VARCHAR(100) NOT NULL, assoc_id BIGINT NOT NULL)", cb);
			break;
		case "sqlite":
			db.run("DROP TABLE IF EXISTS " + table, function () {
				db.run("CREATE TEMPORARY TABLE " + table + " (id SERIAL PRIMARY KEY, name VARCHAR(100) NOT NULL, assoc_id BIGINT NOT NULL)", cb);
			});
			break;
		default:
			db.query("CREATE TEMPORARY TABLE " + table + " (id BIGINT NOT NULL PRIMARY KEY AUTO_INCREMENT, name VARCHAR(100) NOT NULL, assoc_id BIGINT NOT NULL)", cb);
			break;
	}
};

common.createModelAssocTable = function (table, assoc, db, cb) {
	switch (this.protocol()) {
		case "postgres":
		case "redshift":
			db.query("CREATE TEMPORARY TABLE " + table + "_" + assoc + " (" + table + "_id BIGINT NOT NULL, " + assoc + "_id BIGINT NOT NULL, extra_field BIGINT)", cb);
			break;
		case "sqlite":
			db.run("DROP TABLE IF EXISTS " + table + "_" + assoc, function () {
				db.run("CREATE TABLE " + table + "_" + assoc + " (" + table + "_id INTEGER NOT NULL, " + assoc + "_id INTEGER NOT NULL, extra_field INTEGER)", cb);
			});
			break;
		default:
			db.query("CREATE TEMPORARY TABLE " + table + "_" + assoc + " (" + table + "_id BIGINT NOT NULL, " + assoc + "_id BIGINT NOT NULL, extra_field BIGINT)", cb);
			break;
	}
};

common.insertModelData = function (table, db, data, cb) {
	var query = [], i;

	switch (this.protocol()) {
		case "postgres":
		case "redshift":
		case "mysql":
			query = [];

			for (i = 0; i < data.length; i++) {
				query.push(data[i].id + ", '" + data[i].name + "'");
			}

			db.query("INSERT INTO " + table + " VALUES (" + query.join("), (") + ")", cb);
			break;
		case "sqlite":
			var pending = data.length;
			for (i = 0; i < data.length; i++) {
				db.run("INSERT INTO " + table + " VALUES (" + data[i].id + ", '" + data[i].name + "')", function () {
					pending -= 1;

					if (pending === 0) {
						return cb();
					}
				});
			}
			break;
	}
};

common.insertModel2Data = function (table, db, data, cb) {
	var query = [], i;

	switch (this.protocol()) {
		case "postgres":
		case "redshift":
		case "mysql":
			query = [];

			for (i = 0; i < data.length; i++) {
				query.push(data[i].id + ", '" + data[i].name + "', " + data[i].assoc);
			}

			db.query("INSERT INTO " + table + " VALUES (" + query.join("), (") + ")", cb);
			break;
		case "sqlite":
			var pending = data.length;
			for (i = 0; i < data.length; i++) {
				db.run("INSERT INTO " + table + " VALUES (" + data[i].id + ", '" + data[i].name + "', " + data[i].assoc + ")", function () {
					pending -= 1;

					if (pending === 0) {
						return cb();
					}
				});
			}
			break;
	}
};

common.insertModelAssocData = function (table, db, data, cb) {
	var query = [], i;

	switch (this.protocol()) {
		case "postgres":
		case "redshift":
		case "mysql":
			query = [];

			for (i = 0; i < data.length; i++) {
				if (data[i].length < 3) {
					data[i].push(0);
				}
				query.push(data[i].join(", "));
			}

			db.query("INSERT INTO " + table + " VALUES (" + query.join("), (") + ")", cb);
			break;
		case "sqlite":
			var pending = data.length;
			for (i = 0; i < data.length; i++) {
				if (data[i].length < 3) {
					data[i].push(0);
				}
				db.run("INSERT INTO " + table + " VALUES (" + data[i].join(", ") + ")", function () {
					pending -= 1;

					if (pending === 0) {
						return cb();
					}
				});
			}
			break;
	}
};
