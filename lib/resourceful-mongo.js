var url = require('url'),
    util = require('util'),
    resourceful = require('resourceful'),
    mongo = require('mongodb');

exports.connections = {};
exports.deferred = {};

var Mongo = exports.Mongo = function Mongo(config) {
  var self = this;

  // This function is being called to specify a database connection, define a model, or both. If
  // neither a collection name nor an onConnect callback is given, throw an error. 
  if ((!config.collection || typeof config.collection !== 'string') && typeof config.onConnect !== 'function') {
      throw new Error("A collection string or onConnect callback must be specified in the config parameter.");
  }

  if (config.uri) {
    var uri = url.parse('mongodb://' + config.uri, true);
    config.host = uri.hostname;
    config.port = uri.port;
    config.auth = uri.auth;

    if (uri.pathname) {
      config.database = uri.pathname.replace(/^\/|\/$/g, '');
    }

    if (uri.query) {
      resourceful.mixin(config, uri.query);
    }
  }

  config.host     = config.host     || '127.0.0.1';
  config.port     = config.port     || 27017;
  config.database = config.database || resourceful.env || 'test';
  config.safe     = (String(config.safe) === 'true');

  config.uri = url.format({
    protocol: 'mongodb',
    auth: config.auth,  // Will be ignored if null.
    hostname: config.host,
    port: config.port,
    pathname: '/' + config.database
  });

  config.port = parseInt(config.port, 10);

  this.config = config;
  this.cache = new resourceful.Cache();

  // If a connection for this URI has already been made, use it.
  if (exports.connections[config.uri]) {
    this.connection = exports.connections[config.uri];

  // Otherwise if an onConnect callback was given, open a new connection.
  } else if (typeof config.onConnect === 'function') {
    new mongo.Db(config.database, new mongo.Server(config.host, config.port, {})).open(function(err, db) {
      if (err) {
        return config.onConnect(err);
      }

      var onSuccess = function() {
        self.connection = exports.connections[config.uri] = db;

        (function handleDeferredAction(i) {
          if (exports.deferred[config.uri] && i < exports.deferred[config.uri].length) {
            exports.deferred[config.uri][i](config.uri, function() {
                handleDeferredAction(++i);
            });
          } else {
            delete exports.deferred[config.uri];
            config.onConnect();
          }
        })(0);
      };

      if (config.auth) {
        var split = config.auth.split(':');
        db.authenticate(split[0], split[1], function(err, result) {
          if (err) {
            console.log('Authentication failed: ' + err);
            config.onConnect(err);
            process.exit(1);
          }
          onSuccess();
        });
      } else {
        onSuccess();
      }
    });

  // Failing that, just remember to set `this.connection` when the appropriate connection does get made.
  // This makes it easy to define resources before opening their database connection.
  } else {
    var self = this;

    if (!exports.deferred[config.uri]) {
      exports.deferred[config.uri] = [];
    }

    exports.deferred[config.uri].push(function(uri, callback) {
      self.connection = exports.connections[uri];
      callback();
    });
  }
};

Mongo.prototype.protocol = 'mongodb';

// Lazy-load the resource's collection
Mongo.prototype.collection = function(callback) {
  var self = this;

  if (this._collection) {
    return callback(null, this._collection);
  } else {
    this.connection.collection(self.config.collection, function(err, collection) {
      if(err) return callback(err);
      self._collection = collection;
      return callback(null, collection);
    });
  }
};

Mongo.prototype.save = function (id, doc, callback) {
  var args = Array.prototype.slice.call(arguments, 0);
  var callback = args.pop();
  var doc = args.pop();
  var config = this.config;

  this.collection(function(err, collection) {
    if (err) return callback(err);

    if (args.length) {
      doc._id = id;
    }

    if (doc._id && typeof doc._id === 'string') {
      try {
        doc._id = mongo.ObjectID(doc._id);
      } catch (error) {
        return callback(error);
      }
    }

    var onSave = function(err, saved) {
      if (err) return callback(err);

      if (saved._id) {
        doc._id = saved._id.toString();
      }

      callback(null, doc);
    };

    collection.save(doc, {safe: config.safe}, onSave);

    if (doc._id) {
      doc._id = doc._id.toString();
    }
  });
};

Mongo.prototype.update = function (id, doc, callback) {
  var self = this;

  if (id && typeof id === 'string') {
    try {
      id = {'_id': mongo.ObjectID(id)}
    } catch (error) {
      return callback(error);
    }
  }

  var config = this.config;

  this.collection(function(err, collection){
    if (err) return callback(err);

    collection.update(id, {$set: doc}, {safe : config.safe}, function(err) {
      if(err) return callback(err);

      return self.get(id, callback);
    });
  });
};

Mongo.prototype.get = function(id, callback) {

  if (id && typeof id === 'string') {
    try {
      id = {'_id': mongo.ObjectID(id)};
    } catch (error) {
      return callback(error);
    }
  }

  this.collection(function(err, collection) {
    if (err) return callback(err);

    collection.findOne(id, function(err, doc) {
      if (err) return callback(err);

      if (doc && doc._id) {
        doc._id = doc._id.toString();
      }

      callback(null, doc || {});
    });
  });
};

Mongo.prototype.find = function(criteria, callback) {
 
  if (util.isArray(criteria)) {
    try {
      criteria = {
        _id: {'$in': criteria.map(function(ID) {
          return mongo.ObjectID(ID);
        })}
      };
    } catch (error) {
      return callback(error);
    }
  }

  this.collection(function(err, collection) {
    if (err) return callback(err);

    collection.find(criteria).toArray(function(err, docs) {
      if (err) return callback(err);

      callback(null, docs.map(function(doc) {
        doc._id = doc._id.toString();
        return doc
      }));
    });
  });
};

Mongo.prototype.destroy = function(id, callback) {

  if (id && typeof id === 'string') {
    try {
      id = {'_id': mongo.ObjectID(id)};
    } catch (error) {
      return callback(error);
    }
  }

  var config = this.config;
  
  this.collection(function(err, collection) {
    collection.remove(id, {safe : config.safe}, callback);
  });
};

//register engine with resourceful
resourceful.engines.Mongodb = Mongo;

//export resourceful
module.exports = resourceful
