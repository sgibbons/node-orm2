var util           = require("util");
var events         = require("events");
var path           = require("path");
var hat            = require("hat");
var Query          = require("sql-query");
var inflection     = require("inflection");

var Model          = require("./Model").Model;
var DriverAliases  = require("./Drivers/aliases");
var Validators     = require("./Validators");
var Property       = require("./Property");
var Settings       = require("./Settings");

exports.validators = Validators;
exports.settings   = new Settings.Container(Settings.defaults());

for (var k in Query.Comparators) {
	exports[Query.Comparators[k]] = Query[Query.Comparators[k]];
}

exports.express = function () {
	return require("./Express").apply(this, arguments);
};

exports.use = function (connection, proto, opts, cb) {
	if (DriverAliases[proto]) {
		proto = DriverAliases[proto];
	}
	if (typeof opts == "function") {
		cb = opts;
		opts = {};
	}

	try {
		var Driver = require("./Drivers/DML/" + proto).Driver;
		var driver = new Driver(null, connection, {
			debug: (opts.query && opts.query.debug == 'true')
		});

		return cb(null, new ORM(driver, new Settings.Container(exports.settings.get('*'))));
	} catch (ex) {
		return cb(ex);
	}
};

exports.connect = function (opts, cb) {
	var url = require("url");

	if (typeof opts == "string") {
		if (opts.replace(/\s+/, "").length === 0) {
			return cb(new Error("CONNECTION_URL_EMPTY"));
		}
		opts = url.parse(opts, true);
	}
	if (!opts.database) {
		// if (!opts.pathname) {
		// 	return cb(new Error("CONNECTION_URL_NO_DATABASE"));
		// }
		opts.database = (opts.pathname ? opts.pathname.substr(1) : "");
	}
	if (!opts.protocol) {
		return cb(new Error("CONNECTION_URL_NO_PROTOCOL"));
	}
	// if (!opts.host) {
	// 	opts.host = opts.hostname = "localhost";
	// }
	if (opts.auth) {
		opts.user = opts.auth.split(":")[0];
		opts.password = opts.auth.split(":")[1];
	}
	if (!opts.hasOwnProperty("user")) {
		opts.user = "root";
	}
	if (!opts.hasOwnProperty("password")) {
		opts.password = "";
	}

	var proto  = opts.protocol.replace(/:$/, '');
	if (DriverAliases[proto]) {
		proto = DriverAliases[proto];
	}

	try {
		var Driver = require("./Drivers/DML/" + proto).Driver;
		var debug  = Boolean(extractOption(opts, "debug"));
		var pool   = Boolean(extractOption(opts, "pool"));
		var driver = new Driver(opts, null, {
			debug : debug,
			pool  : pool
		});

		driver.connect(function (err) {
			if (err) {
				return cb(err);
			}

			return cb(null, new ORM(driver, new Settings.Container(exports.settings.get('*'))));
		});
	} catch (ex) {
		return cb(ex);
	}
};

function ORM(driver, settings) {
	this.validators = Validators;
	this.settings   = settings;
	this.driver     = driver;
	this.driver.uid = hat();
	this.tools      = {};
	this.models     = {};

	for (var k in Query.Comparators) {
		this.tools[Query.Comparators[k]] = Query[Query.Comparators[k]];
	}

	events.EventEmitter.call(this);

	var onError = function (err) {
		if (this.settings.get("connection.reconnect")) {
			if (typeof this.driver.reconnect == "undefined") {
				return this.emit("error", new Error("Connection lost - driver does not support reconnection"));
			}
			this.driver.reconnect(function () {
				this.driver.on("error", onError);
			}.bind(this));

			if (this.listeners("error").length === 0) {
				// since user want auto reconnect,
				// don't emit without listeners or it will throw
				return;
			}
		}
		this.emit("error", err);
	}.bind(this);

	driver.on("error", onError);
}

util.inherits(ORM, events.EventEmitter);

ORM.prototype.define = function (name, properties, opts, callback) {
	properties = properties || {};
	opts       = opts || {};

  var buildModel = function(properties, settings, driver, models, callback) {
    for (var k in properties) {
      properties[k] = Property.check(properties[k], settings);
    }

    models[name] = new Model({
      settings       : settings,
      driver         : driver,
      table          : opts.table || opts.collection || name,
      properties     : properties,
      indexes        : opts.indexes || [],
      cache          : opts.hasOwnProperty("cache") ? opts.cache : settings.get("instance.cache"),
      id             : opts.id || settings.get("properties.primary_key"),
      autoSave       : opts.hasOwnProperty("autoSave") ? opts.autoSave : settings.get("instance.autoSave"),
      autoFetch      : opts.hasOwnProperty("autoFetch") ? opts.autoFetch : settings.get("instance.autoFetch"),
      autoFetchLimit : opts.autoFetchLimit || settings.get("instance.autoFetchLimit"),
      cascadeRemove  : opts.hasOwnProperty("cascadeRemove") ? opts.cascadeRemove : settings.get("instance.cascadeRemove"),
      hooks          : opts.hooks || {},
      methods        : opts.methods || {},
      validations    : opts.validations || {}
    });
    if(callback) {
      callback(models[name]);
    } else {
      return models[name];
    }
  };

  if(opts.infer) {
    if(!callback) {
      throw("When using automatic model inference, a callback must be provided to handle asynchronous results");
    }
    properties = this.driver.infer(opts.table || inflection.tableize(name), function(properties) {
      buildModel(properties, this.settings, this.driver, this.models, callback)
    });
  } else {
    return buildModel(properties, this.settings, this.driver, this.models);
  }
};
ORM.prototype.ping = function (cb) {
	this.driver.ping(cb);

	return this;
};
ORM.prototype.close = function (cb) {
	this.driver.close(cb);

	return this;
};
ORM.prototype.load = function (file, cb) {
	var cwd = process.cwd();
	var err = new Error();
	var tmp = err.stack.split(/\r?\n/)[2], m;

	if ((m = tmp.match(/^\s*at\s+(.+):\d+:\d+$/)) !== null) {
		cwd = path.dirname(m[1]);
	} else if ((m = tmp.match(/^\s*at\s+module\.exports\s+\((.+?)\)/)) !== null) {
		cwd = path.dirname(m[1]);
	}

	if (file[0] != path.sep) {
		file = cwd + "/" + file;
	}
	if (file.substr(-1) == path.sep) {
		file += "index";
	}

	try {
		require(file)(this, cb);
	} catch (ex) {
		return cb(ex);
	}
};
ORM.prototype.sync = function (cb) {
	var modelIds = Object.keys(this.models);
	var syncNext = function () {
		if (modelIds.length === 0) {
			return cb();
		}

		var modelId = modelIds.shift();

		this.models[modelId].sync(function (err) {
			if (err) {
				err.model = modelId;

				return cb(err);
			}

			return syncNext();
		});
	}.bind(this);

	if (arguments.length === 0) {
		cb = function () {};
	}

	syncNext();

	return this;
};
ORM.prototype.serial = function () {
	var chains = Array.prototype.slice.apply(arguments);

	return {
		get: function (cb) {
			var params = [];
			var getNext = function () {
				if (params.length === chains.length) {
					params.unshift(null);
					return cb.apply(null, params);
				}

				chains[params.length].run(function (err, instances) {
					if (err) {
						params.unshift(err);
						return cb.apply(null, params);
					}

					params.push(instances);
					return getNext();
				});
			};

			getNext();

			return this;
		}
	};
};

function extractOption(opts, key) {
	if (!opts.query.hasOwnProperty(key)) {
		return null;
	}

	var opt = opts.query[key];

	delete opts.query[key];
	if (opts.href) {
		opts.href = opts.href.replace(new RegExp(key + "=[^&]+&?"), "");
	}
	if (opts.search) {
		opts.search = opts.search.replace(new RegExp(key + "=[^&]+&?"), "");
	}
	return opt;
}
