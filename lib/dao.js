// Generated by CoffeeScript 1.8.0
(function() {
  var Dao, connect, escape, lineUp, pg, _;

  _ = require('lodash');

  pg = require('pg');

  connect = function(o, cb) {
    o.log({
      level: 'debug',
      msg: 'daobase.connect request',
      data: {
        dbUrl: o.dbUrl
      }
    });
    return pg.connect(o.dbUrl, function(err, client, done) {
      if (err) {
        err = {
          type: 'DAOBASE_SQL_CONNECT_ERROR',
          err: err
        };
        o.log({
          level: 'emerg',
          err: err
        });
        return cb(err);
      }
      o._client = client;
      o._done = function() {
        o._client = void 0;
        return done();
      };
      o.log({
        level: 'debug',
        msg: 'daobase.connect success'
      });
      return cb(null);
    });
  };

  escape = function(arr) {
    var elem;
    return (function() {
      var _i, _len, _results;
      _results = [];
      for (_i = 0, _len = arr.length; _i < _len; _i++) {
        elem = arr[_i];
        _results.push('"' + elem + '"');
      }
      return _results;
    })();
  };

  lineUp = function(fields, values) {
    var f;
    if (_.isArray(values)) {
      return values;
    }
    if (fields.length === 1) {
      return [values];
    }
    return (function() {
      var _i, _len, _results;
      _results = [];
      for (_i = 0, _len = fields.length; _i < _len; _i++) {
        f = fields[_i];
        _results.push(values[f]);
      }
      return _results;
    })();
  };

  Dao = (function() {
    function Dao(dbUrl, log) {
      this.dbUrl = dbUrl;
      this._log = log;
      this.log({
        level: 'debug',
        msg: 'daobase constructed'
      });
    }

    Dao.prototype.log = function(obj) {
      if (!this._log) {
        return;
      }
      return this._log[obj.level](obj);
    };

    Dao.prototype.sqlOp = function(sql, cb) {
      var self;
      self = this;
      this.log({
        level: 'debug',
        msg: 'daobase.sqlOp request',
        data: {
          query: sql
        }
      });
      if (!_.isArray(sql)) {
        sql = [sql];
      }
      sql.push(function(err, result) {
        if (err) {
          err = {
            type: 'DAOBASE_GENERIC_SQL_ERROR',
            err: err,
            query: sql
          };
          self.log({
            level: 'err',
            err: err
          });
          self._done(err);
          return cb(err);
        }
        self.log({
          level: 'debug',
          msg: 'daobase.sqlOp success',
          data: {
            query: sql,
            result: result
          }
        });
        return cb(null, result);
      });
      if (!this._client) {
        return connect(this, function(err, result) {
          if (err) {
            return cb(err);
          }
          return self.sqlOp(sql, function(err, result) {
            if (err) {
              return cb(err);
            }
            self._done();
            return cb(null, result);
          });
        });
      } else {
        return this._client.query.apply(this._client, sql);
      }
    };

    Dao.prototype.commit = function(cb) {
      var self;
      self = this;
      this.log({
        msg: 'daobase.commit request',
        level: 'debug'
      });
      return this.sqlOp('COMMIT', function(err, result) {
        if (err) {
          err = {
            type: 'DAOBASE_COMMIT_ERROR',
            err: err
          };
          self.log({
            level: 'err',
            err: err
          });
          return cb(err);
        }
        self._done();
        self.log({
          level: 'debug',
          msg: 'daobase.commit success'
        });
        return cb();
      });
    };

    Dao.prototype.get = function(data, cb) {
      var err, self;
      self = this;
      this.log({
        msg: 'daobase.get request',
        level: 'debug',
        data: data
      });
      if (!((data != null) && data.table && data.fields && data.values)) {
        err = {
          type: 'DAOBASE_GET_INVALID_REQUEST',
          data: data
        };
        this.log({
          level: 'err',
          err: err
        });
        return cb(err);
      }
      return this.getMulti(data, function(err, result) {
        if (err) {
          err = {
            type: 'DAOBASE_GET_ERROR',
            err: err
          };
          self.log({
            level: 'err',
            err: err
          });
          return cb(err);
        }
        if (result.length < 1) {
          err = {
            type: 'DAOBASE_GET_NO_MATCH',
            data: data
          };
          self.log({
            level: 'err',
            err: err
          });
          return cb(err);
        }
        if (result.length > 1) {
          err = {
            type: 'DAOBASE_GET_MULTIPLE_MATCHES',
            data: data
          };
          self.log({
            level: 'err',
            err: err
          });
          return cb(err);
        }
        self.log({
          level: 'debug',
          msg: 'daobase.get success',
          data: {
            query: data,
            response: result[0]
          }
        });
        return cb(null, result[0]);
      });
    };

    Dao.prototype.getMulti = function(data, cb) {
      var err, f, query, s, self, sql;
      self = this;
      this.log({
        msg: 'daobase.getMulti request',
        level: 'debug',
        data: data
      });
      if (!((data != null) && data.table)) {
        err = {
          type: 'DAOBASE_GETMULTI_INVALID_REQUEST',
          data: data
        };
        this.log({
          level: 'err',
          err: err
        });
        return cb(err);
      }
      if (data.fields == null) {
        data.fields = [];
      }
      if (data.values == null) {
        data.values = [];
      }
      if (!(_.isArray(data.fields))) {
        data.fields = [data.fields];
      }
      data.values = lineUp(data.fields, data.values);
      data.fields = escape(data.fields);
      query = 'SELECT * FROM "' + data.table + '" ';
      if (data.fields.length > 0) {
        s = ((function() {
          var _i, _j, _len, _ref, _ref1, _results, _results1;
          _ref1 = _.zip(data.fields, (function() {
            _results1 = [];
            for (var _j = 1, _ref = data.values.length; 1 <= _ref ? _j <= _ref : _j >= _ref; 1 <= _ref ? _j++ : _j--){ _results1.push(_j); }
            return _results1;
          }).apply(this));
          _results = [];
          for (_i = 0, _len = _ref1.length; _i < _len; _i++) {
            f = _ref1[_i];
            _results.push(f[0] + '=$' + f[1]);
          }
          return _results;
        })()).join(' AND ');
        if (s.length !== 0) {
          query += "WHERE " + s;
        }
      }
      sql = [query, data.values];
      return this.sqlOp(sql, function(err, result) {
        if (err) {
          err = {
            type: 'DAOBASE_GETMULTI_ERROR',
            err: err
          };
          self.log({
            level: 'err',
            err: err
          });
          return cb(err);
        }
        self.log({
          level: 'debug',
          msg: 'daobase.getMulti success',
          data: {
            query: data,
            response: result.rows
          }
        });
        return cb(null, result.rows);
      });
    };

    Dao.prototype.insert = function(data, cb) {
      var err, field, i, query, self;
      self = this;
      this.log({
        msg: 'daobase.insert request',
        level: 'debug',
        data: data
      });
      if (!((data != null) && data.table && data.values)) {
        err = {
          type: 'DAOBASE_INSERT_INVALID_REQUEST',
          data: data
        };
        this.log({
          level: 'err',
          err: err
        });
        return cb(err);
      }
      if (data.fields) {
        if (!_.isArray(data.fields)) {
          data.fields = [data.fields];
        }
        data.values = lineUp(data.fields, data.values);
      }
      if (!(data.fields == null)) {
        data.fields = escape(data.fields);
      }
      query = 'INSERT INTO "' + data.table + '" ';
      if (!(data.fields == null)) {
        query += '(' + ((function() {
          var _i, _len, _ref, _results;
          _ref = data.fields;
          _results = [];
          for (_i = 0, _len = _ref.length; _i < _len; _i++) {
            field = _ref[_i];
            _results.push(field);
          }
          return _results;
        })()) + ') ';
      }
      query += 'VALUES (' + ((function() {
        var _i, _ref, _results;
        _results = [];
        for (i = _i = 1, _ref = data.values.length; 1 <= _ref ? _i <= _ref : _i >= _ref; i = 1 <= _ref ? ++_i : --_i) {
          _results.push('$' + i);
        }
        return _results;
      })()) + ')';
      if (!(data.returning == null)) {
        query += ' RETURNING "' + data.returning + '"';
      }
      return this.sqlOp([query, data.values], function(err, result) {
        if (err) {
          err = {
            type: 'DAOBASE_INSERT_ERROR',
            err: err,
            data: data
          };
          self.log({
            err: err,
            level: 'err'
          });
          return cb(err);
        }
        result = data.returning ? result.rows[0][data.returning] : true;
        self.log({
          msg: 'daobase.insert success',
          level: 'debug',
          data: {
            query: data,
            response: result
          }
        });
        return cb(null, result);
      });
    };

    Dao.prototype.update = function(data, cb) {
      var err, f, nSel, nUpd, query, self, sql;
      self = this;
      this.log({
        msg: 'daobase.update request',
        level: 'debug',
        data: data
      });
      if (!((data != null) && data.table && data.updFields && data.updValues && data.selFields && data.selValues)) {
        err = {
          type: 'DAOBASE_UPDATE_INVALID_REQUEST',
          data: data
        };
        this.log({
          err: err,
          level: 'err'
        });
        return cb(err);
      }
      if (!_.isArray(data.updFields)) {
        data.updFields = [data.updFields];
      }
      if (!_.isArray(data.selFields)) {
        data.selFields = [data.selFields];
      }
      data.updValues = lineUp(data.updFields, data.updValues);
      data.selValues = lineUp(data.selFields, data.selValues);
      data.updFields = escape(data.updFields);
      data.selFields = escape(data.selFields);
      nUpd = data.updValues.length;
      nSel = data.selValues.length;
      sql = 'UPDATE "' + data.table + '" SET ' + (((function() {
        var _i, _j, _len, _ref, _results, _results1;
        _ref = _.zip(data.updFields, (function() {
          _results1 = [];
          for (var _j = 1; 1 <= nUpd ? _j <= nUpd : _j >= nUpd; 1 <= nUpd ? _j++ : _j--){ _results1.push(_j); }
          return _results1;
        }).apply(this));
        _results = [];
        for (_i = 0, _len = _ref.length; _i < _len; _i++) {
          f = _ref[_i];
          _results.push(f[0] + '=$' + f[1]);
        }
        return _results;
      })()).join(',')) + ' WHERE ' + ((function() {
        var _i, _j, _len, _ref, _ref1, _ref2, _results, _results1;
        _ref2 = _.zip(data.selFields, (function() {
          _results1 = [];
          for (var _j = _ref = nUpd + 1, _ref1 = nUpd + nSel; _ref <= _ref1 ? _j <= _ref1 : _j >= _ref1; _ref <= _ref1 ? _j++ : _j--){ _results1.push(_j); }
          return _results1;
        }).apply(this));
        _results = [];
        for (_i = 0, _len = _ref2.length; _i < _len; _i++) {
          f = _ref2[_i];
          _results.push(f[0] + '=$' + f[1]);
        }
        return _results;
      })()).join(' AND ');
      query = [sql, data.updValues.concat(data.selValues)];
      return this.sqlOp(query, function(err, result) {
        if (err) {
          err = {
            type: 'DAOBASE_UPDATE_ERROR',
            err: err,
            data: data
          };
          self.log({
            err: err,
            level: 'err'
          });
          return cb(err);
        }
        self.log({
          msg: 'daobase.update success',
          level: 'debug',
          data: {
            query: data,
            response: result.rowCount
          }
        });
        return cb(null, result.rowCount);
      });
    };

    Dao.prototype["delete"] = function(data, cb) {
      var err, f, self, sql;
      self = this;
      this.log({
        msg: 'daobase.delete request',
        level: 'debug',
        data: data
      });
      if (!((data != null) && data.table && data.fields && data.values)) {
        err = {
          type: 'DAOBASE_DELETE_INVALID_REQUEST',
          data: data
        };
        this.log({
          err: err,
          level: 'err'
        });
        return cb(err);
      }
      if (!_.isArray(data.fields)) {
        data.fields = [data.fields];
      }
      data.values = lineUp(data.fields, data.values);
      data.fields = escape(data.fields);
      sql = 'DELETE FROM "' + data.table + '" WHERE ' + ((function() {
        var _i, _j, _len, _ref, _ref1, _results, _results1;
        _ref1 = _.zip(data.fields, (function() {
          _results1 = [];
          for (var _j = 1, _ref = data.fields.length; 1 <= _ref ? _j <= _ref : _j >= _ref; 1 <= _ref ? _j++ : _j--){ _results1.push(_j); }
          return _results1;
        }).apply(this));
        _results = [];
        for (_i = 0, _len = _ref1.length; _i < _len; _i++) {
          f = _ref1[_i];
          _results.push(f[0] + '=$' + f[1]);
        }
        return _results;
      })()).join(' AND ');
      return this.sqlOp([sql, data.values], function(err, result) {
        if (err) {
          err = {
            type: 'DAOBASE_DELETE_ERROR',
            err: err,
            data: data
          };
          self.log({
            err: err,
            level: 'err'
          });
          return cb(err);
        }
        if (result.rowCount < 1) {
          err = {
            type: 'DAOBASE_DELETE_NO_MATCH',
            data: data
          };
          self.log({
            err: err,
            level: 'err'
          });
          return cb(err);
        }
        self.log({
          msg: 'daobase.delete success',
          level: 'debug',
          data: {
            query: data,
            response: result.rowCount
          }
        });
        return cb(null, result.rowCount);
      });
    };

    Dao.prototype.tx = function(cb) {
      var d, self;
      self = this;
      this.log({
        msg: 'daobase.tx request',
        level: 'debug'
      });
      d = new this.constructor(this.dbUrl, this._log);
      return connect(d, function(err) {
        if (err) {
          return cb(err);
        }
        return d.sqlOp('BEGIN', function(err) {
          if (err) {
            err = {
              type: 'DAOBASE_BEGIN_ERROR',
              err: err
            };
            self.log({
              err: err,
              level: 'err'
            });
            return cb(err);
          }
          self.log({
            msg: 'daobase.tx success',
            level: 'debug'
          });
          return cb(null, d);
        });
      });
    };

    return Dao;

  })();

  module.exports = Dao;

}).call(this);
