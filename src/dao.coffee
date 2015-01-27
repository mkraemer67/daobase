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

class Dao
    constructor: (@dbUrl) ->
        this.dbUrl = @dbUrl

    sqlOp: (sql, cb) ->
        self = this
        if not _.isArray sql
            sql = [sql]
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

    commit: (cb) ->
        self = this
        this.sqlOp 'COMMIT', (err, result) ->
            if err
                err =
                    type : 'DAOBASE_COMMIT_ERROR'
                    err  : err
                return cb err
            self._done()
            cb()

    get: (data, cb) ->
        if not (data? and data.table and data.fields and data.values)
            err =
                type : 'DAOBASE_GET_INVALID_REQUEST'
                data : data
            return cb err
        if not (_.isArray data.fields)
            data.fields = [data.fields]
            data.values = [data.values]
        query = 'SELECT * FROM ' + data.table + ' WHERE ' +
            (f[0] + '=$' + f[1] for f in _.zip data.fields, [1..data.values.length]).join ' AND '
        sql = [query, data.values]
        this.sqlOp sql, (err, result) ->
            if err
                err =
                    type : 'DAOBASE_GET_ERROR'
                    err  : err
                return cb err
            if result.rows.length < 1
                err =
                    type : 'DAOBASE_GET_NO_MATCH'
                    data : data
                return cb err
            if result.rows.length > 1
                err =
                    type : 'DAOBASE_GET_MULTIPLE_MATCHES'
                    data : data
                return cb err
            cb null, result.rows[0]

    # todo : getMulti

    insert: (data, cb) ->
        if not (data? and data.table and data.values)
            err =
                type : 'DAOBASE_INSERT_INVALID_REQUEST'
                data : data
            return cb err
        if not _.isArray data.values
            data.values = [data.values]
            data.fields = [data.fields] unless not data.fields?
        query = 'INSERT INTO ' + data.table + ' '
        query += '(' + (field for field in data.fields) + ') ' unless not data.fields?
        query += 'VALUES (' + ('$' + i for i in [1..data.values.length]) + ')'
        query += ' RETURNING ' + data.returning unless not data.returning?
        this.sqlOp [query, data.values], (err, result) ->
            if err
                err =
                    type : 'DAOBASE_INSERT_ERROR'
                    err  : err
                    data : data
                return cb err
            result = if data.returning then result.rows[0][data.returning] else true
            cb null, result

    # todo : limit to single row update and provide updateMulti
    update: (data, cb) ->
        if not (data? and data.table and data.updFields and data.updValues and
            data.selFields and data.selValues)
                err =
                    type : 'DAOBASE_UPDATE_INVALID_REQUEST'
                    data : data
                return cb err
        if not _.isArray data.updFields
            data.updFields = [data.updFields]
            data.updValues = [data.updValues]
        if not _.isArray data.selFields
            data.selFields = [data.selFields]
            data.selValues = [data.selValues]
        nUpd = data.updValues.length
        nSel = data.selValues.length
        sql = 'UPDATE ' + data.table + ' SET ' +
            ((f[0] + '=$' + f[1] for f in _.zip data.updFields, [1..nUpd]).join ',') + ' WHERE ' +
            (f[0] + '=$' + f[1] for f in _.zip data.selFields, [nUpd+1..nUpd+nSel]).join ' AND '
        query = [sql, data.updValues.concat data.selValues]
        this.sqlOp query, (err, result) ->
            if err
                err =
                    type : 'DAOBASE_UPDATE_ERROR'
                    err  : err
                    data : data
                return cb err
            cb null, result.rowCount

    # todo : updateMulti

    # todo : limit to single row delete and provide deleteMulti
    delete: (data, cb) ->
        if not (data? and data.table and data.fields and data.values)
            err =
                type : 'DAOBASE_DELETE_INVALID_REQUEST'
                data : data
            return cb err
        if not _.isArray data.fields
            data.fields = [data.fields]
            data.values = [data.values]
        sql = 'DELETE FROM ' + data.table + ' WHERE ' +
            (f[0] + '=$' + f[1] for f in _.zip data.fields, [1..data.fields.length]).join ' AND '
        this.sqlOp [sql, data.values], (err, result) ->
            if err
                err =
                    type : 'DAOBASE_DELETE_ERROR'
                    err  : err
                    data : data
                return cb err
            if result.rowCount < 1
                err =
                    type : 'DAOBASE_DELETE_NO_MATCH'
                    data : data
                return cb err
            cb null, result.rowCount

    tx: (cb) ->
        d = new Dao()
        d = _.assign d, this
        d.dbUrl = this.dbUrl
        connect d, (err) ->
            if err
                return cb err
            d.sqlOp 'BEGIN', (err) ->
                if err
                    err =
                        type : 'DAOBASE_BEGIN_ERROR'
                        err  : err
                    return cb err
                cb null, d

module.exports = Dao
