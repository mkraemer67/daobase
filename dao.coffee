_ = require 'lodash'
pg = require 'pg'

connect = (o, cb) ->
    pg.connect o.dbUrl, (err, client, done) ->
        if err
            err =
                type : 'DAOBASE_SQL_CONNECT_ERROR'
                err  : err
            return cb err
        o._client = client
        o._done = ->
            o._client = undefined
            done()
        cb null

dao = {}

Dao = (dbUrl) ->
    _.assign this, dao
    this.dbUrl = dbUrl

dao.sqlOp = (sql, cb) ->
    self = this
    sql.push (err, result) ->
        if err
            err =
                type  : 'DAOBASE_GENERIC_SQL_ERROR'
                err   : err
                query : sql
            self._done err
            return cb err
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

dao.commit = (cb) ->
    self = this
    this.sqlOp ['COMMIT'], (err, result) ->
        if err
            err =
                type : 'DAOBASE_COMMIT_ERROR'
                err  : err
            return cb err
        self._done()
        cb()

dao.get = (data, cb) ->
    if not (data? and data.table and data.field and data.value)
        err =
            type : 'DAOBASE_GET_INVALID_REQUEST'
            data : data
        return cb err
    sql = ['SELECT * FROM ' + data.table + ' WHERE ' + data.field + '=$1', [data.value]]
    this.sqlOp sql, (err, result) ->
        if err
            err =
                type : 'DAOBASE_GET_ERROR'
                err  : err
            return cb err
        if result.rows.length < 1
            err =
                type : 'DAOBASE_GET_ROW_NOT_FOUND'
                data : data
            return cb err
        cb null, result.rows[0]

dao.insert = (data, cb) ->
    if not (data? and data.table and data.values)
        err =
            type : 'DAOBASE_INSERT_INVALID_REQUEST'
            data : data
        return cb err
    query = 'INSERT INTO ' + data.table + ' '
    if data.fields
        query += '(' + (field for field in data.fields) + ') '
    query += 'VALUES (' + ('$' + i for i in [1..data.values.length]) + ')'
    if data.returning
        query += ' RETURNING ' + data.returning
    this.sqlOp [query, data.values], (err, result) ->
        if err
            err =
                type : 'DAOBASE_INSERT_ERROR'
                err  : err
                data : data
            return cb err
        result = if data.returning then result.rows[0][data.returning] else true
        cb null, result

dao.tx = (cb) ->
    d = new Dao()
    d.dbUrl = this.dbUrl
    connect d, (err) ->
        if err
            return cb err
        d.sqlOp ['BEGIN'], (err) ->
            if err
                err =
                    type : 'DAOBASE_BEGIN_ERROR'
                    err  : err
                return cb err
            cb null, d

module.exports = Dao
