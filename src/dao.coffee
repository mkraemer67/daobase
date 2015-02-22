_ = require 'lodash'
pg = require 'pg'

connect = (o, cb) ->
    o.log
        level : 'debug'
        msg   : 'daobase.connect request'
        data  :
            dbUrl : o.dbUrl
    pg.connect o.dbUrl, (err, client, done) ->
        if err
            err =
                type : 'DAOBASE_SQL_CONNECT_ERROR'
                err  : err
            o.log
                level : 'emerg'
                err   : err
            return cb err
        o._client = client
        o._done = ->
            o._client = undefined
            done()
        o.log
            level : 'debug'
            msg   : 'daobase.connect success'
        cb null

escape = (arr) ->
    return ('"' + elem + '"' for elem in arr)

lineUp = (fields, values) ->
    if _.isArray values
        return values
    if fields.length is 1
        return [values]
    return (values[f] for f in fields)

class Dao
    constructor: (dbUrl, log) ->
        this.dbUrl = dbUrl
        this._log = log
        this.log
            level : 'debug'
            msg   : 'daobase constructed'

    log: (obj) ->
        if not this._log
            return
        this._log[obj.level] obj

    sqlOp: (sql, cb) ->
        self = this
        this.log
            level : 'debug'
            msg   : 'daobase.sqlOp request'
            data  :
                query : sql
        if not _.isArray sql
            sql = [sql]
        sql.push (err, result) ->
            if err
                err =
                    type  : 'DAOBASE_GENERIC_SQL_ERROR'
                    err   : err
                    query : sql
                self.log
                    level : 'err'
                    err   : err
                self._done err
                return cb err
            self.log
                level : 'debug'
                msg   : 'daobase.sqlOp success'
                data  :
                    query  : sql
                    result : result
            cb null, result
        if not this._client
            connect this, (err, result) ->
                if err
                    return cb err
                self.sqlOp sql, (err, result) ->
                    if err
                        return cb err
                    self._done()
                    cb null, result
        else
            this._client.query.apply this._client, sql

    commit: (cb) ->
        self = this
        this.log
            msg   : 'daobase.commit request'
            level : 'debug'
        this.sqlOp 'COMMIT', (err, result) ->
            if err
                err =
                    type : 'DAOBASE_COMMIT_ERROR'
                    err  : err
                self.log
                    level : 'err'
                    err   : err
                return cb err
            self._done()
            self.log
                level : 'debug'
                msg   : 'daobase.commit success'
            cb()

    get: (data, cb) ->
        self = this
        this.log
            msg   : 'daobase.get request'
            level : 'debug'
            data  : data
        if not (data? and data.table and data.fields and data.values)
            err =
                type : 'DAOBASE_GET_INVALID_REQUEST'
                data : data
            this.log
                level : 'err'
                err   : err
            return cb err
        if not (_.isArray data.fields)
            data.fields = [data.fields]
        data.values = lineUp data.fields, data.values
        data.fields = escape data.fields
        query = 'SELECT * FROM "' + data.table + '" WHERE ' +
            (f[0] + '=$' + f[1] for f in _.zip data.fields, [1..data.values.length]).join ' AND '
        sql = [query, data.values]
        this.sqlOp sql, (err, result) ->
            if err
                err =
                    type : 'DAOBASE_GET_ERROR'
                    err  : err
                self.log
                    level : 'err'
                    err   : err
                return cb err
            if result.rows.length < 1
                err =
                    type : 'DAOBASE_GET_NO_MATCH'
                    data : data
                self.log
                    level : 'err'
                    err   : err
                return cb err
            if result.rows.length > 1
                err =
                    type : 'DAOBASE_GET_MULTIPLE_MATCHES'
                    data : data
                self.log
                    level : 'err'
                    err   : err
                return cb err
            self.log
                level : 'debug'
                msg   : 'daobase.get success'
                data  :
                    query    : data
                    response : result.rows[0]
            cb null, result.rows[0]

    getMulti: (data, cb) ->
        return

    insert: (data, cb) ->
        self = this
        this.log
            msg   : 'daobase.insert request'
            level : 'debug'
            data  : data
        if not (data? and data.table and data.values)
            err =
                type : 'DAOBASE_INSERT_INVALID_REQUEST'
                data : data
            this.log
                level : 'err'
                err   : err
            return cb err
        if data.fields
            data.fields = [data.fields] unless _.isArray data.fields
            data.values = lineUp data.fields, data.values
        data.fields = escape data.fields unless not data.fields?
        query = 'INSERT INTO "' + data.table + '" '
        query += '(' + (field for field in data.fields) + ') ' unless not data.fields?
        query += 'VALUES (' + ('$' + i for i in [1..data.values.length]) + ')'
        query += ' RETURNING "' + data.returning + '"' unless not data.returning?
        this.sqlOp [query, data.values], (err, result) ->
            if err
                err =
                    type : 'DAOBASE_INSERT_ERROR'
                    err  : err
                    data : data
                self.log
                    err   : err
                    level : 'err'
                return cb err
            result = if data.returning then result.rows[0][data.returning] else true
            self.log
                msg   : 'daobase.insert success'
                level : 'debug'
                data  :
                    query    : data
                    response : result
            cb null, result

    # todo : limit to single row update and provide updateMulti
    update: (data, cb) ->
        self = this
        this.log
            msg   : 'daobase.update request'
            level : 'debug'
            data  : data
        if not (data? and data.table and data.updFields and data.updValues and
            data.selFields and data.selValues)
                err =
                    type : 'DAOBASE_UPDATE_INVALID_REQUEST'
                    data : data
                this.log
                    err   : err
                    level : 'err'
                return cb err
        data.updFields = [data.updFields] unless _.isArray data.updFields
        data.selFields = [data.selFields] unless _.isArray data.selFields
        data.updValues = lineUp data.updFields, data.updValues
        data.selValues = lineUp data.selFields, data.selValues
        data.updFields = escape data.updFields
        data.selFields = escape data.selFields
        nUpd = data.updValues.length
        nSel = data.selValues.length
        sql = 'UPDATE "' + data.table + '" SET ' +
            ((f[0] + '=$' + f[1] for f in _.zip data.updFields, [1..nUpd]).join ',') + ' WHERE ' +
            (f[0] + '=$' + f[1] for f in _.zip data.selFields, [nUpd+1..nUpd+nSel]).join ' AND '
        query = [sql, data.updValues.concat data.selValues]
        this.sqlOp query, (err, result) ->
            if err
                err =
                    type : 'DAOBASE_UPDATE_ERROR'
                    err  : err
                    data : data
                self.log
                    err   : err
                    level : 'err'
                return cb err
            self.log
                msg   : 'daobase.update success'
                level : 'debug'
                data  :
                    query    : data
                    response : result.rowCount
            cb null, result.rowCount

    # todo : updateMulti

    # todo : limit to single row delete and provide deleteMulti
    delete: (data, cb) ->
        self = this
        this.log
            msg   : 'daobase.delete request'
            level : 'debug'
            data  : data
        if not (data? and data.table and data.fields and data.values)
            err =
                type : 'DAOBASE_DELETE_INVALID_REQUEST'
                data : data
            this.log
                err   : err
                level : 'err'
            return cb err
        data.fields = [data.fields] unless _.isArray data.fields
        data.values = lineUp data.fields, data.values
        data.fields = escape data.fields
        sql = 'DELETE FROM "' + data.table + '" WHERE ' +
            (f[0] + '=$' + f[1] for f in _.zip data.fields, [1..data.fields.length]).join ' AND '
        this.sqlOp [sql, data.values], (err, result) ->
            if err
                err =
                    type : 'DAOBASE_DELETE_ERROR'
                    err  : err
                    data : data
                self.log
                    err   : err
                    level : 'err'
                return cb err
            if result.rowCount < 1
                err =
                    type : 'DAOBASE_DELETE_NO_MATCH'
                    data : data
                self.log
                    err   : err
                    level : 'err'
                return cb err
            self.log
                msg   : 'daobase.delete success'
                level : 'debug'
                data  :
                    query    : data
                    response : result.rowCount
            cb null, result.rowCount

    tx: (cb) ->
        self = this
        this.log
            msg   : 'daobase.tx request'
            level : 'debug'
        d = new this.constructor this.dbUrl, this._log
        connect d, (err) ->
            if err
                return cb err
            d.sqlOp 'BEGIN', (err) ->
                if err
                    err =
                        type : 'DAOBASE_BEGIN_ERROR'
                        err  : err
                    self.log
                        err   : err
                        level : 'err'
                    return cb err
                self.log
                    msg   : 'daobase.tx success'
                    level : 'debug'
                cb null, d

module.exports = Dao