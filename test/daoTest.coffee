chai = require 'chai'
should = chai.should()

dbUrl = 'postgres://postgres:postgres@localhost/test'

log =
    err   : (obj) -> console.log JSON.stringify obj
    debug : (obj) -> console.log JSON.stringify obj

describe 'daobase', ->
    dao = require '../src/dao'
    dao = new dao dbUrl, log

    it 'should drop table if exists', (done) ->
        query = 'DROP TABLE IF EXISTS daobase_test'
        dao.sqlOp query, (err, result) ->
            should.not.exist err
            done()

    it 'should create a table', (done) ->
        query =
            'CREATE TABLE daobase_test (
               id serial,
               string text,
               number integer
             )'
        dao.sqlOp query, (err, result) ->
            should.not.exist err
            done()

    it 'get should not find a row', (done) ->
        dao.get
            table  : 'daobase_test'
            fields : 'id'
            values : 1
            (err, result) ->
                err.should.be.truthy
                err.type.should.equal 'DAOBASE_GET_NO_MATCH'
                should.not.exist result
                done()

    it 'should insert a row', (done) ->
        dao.insert
            table  : 'daobase_test'
            values : [1, 'test', 0]
            (err, result) ->
                should.not.exist err
                result.should.be.true
                done()

    it 'should get the row', (done) ->
        dao.get
            table  : 'daobase_test'
            fields : ['id', 'number']
            values : [1, 0]
            (err, result) ->
                should.not.exist err
                result.id.should.equal 1
                result.string.should.equal 'test'
                result.number.should.equal 0
                done()

    it 'should not find due to wrong select', (done) ->
        dao.get
            table  : 'daobase_test'
            fields : ['id', 'number']
            values : [1, 99]
            (err, result) ->
                err.should.be.truthy
                err.type.should.equal 'DAOBASE_GET_NO_MATCH'
                should.not.exist result
                done()

    it 'should update the row', (done) ->
        dao.update
            table     : 'daobase_test'
            updFields : ['string', 'number']
            updValues : ['100', 100]
            selFields : 'id'
            selValues : 1
            (err, result) ->
                console.log err, result
                should.not.exist.err
                result.should.equal 1
                done()

    it 'should reflect the updates', (done) ->
        dao.get
            table  : 'daobase_test'
            fields : 'id'
            values : 1
            (err, result) ->
                should.not.exist err
                result.id.should.equal 1
                result.string.should.equal '100'
                result.number.should.equal 100
                done()

    tx = undefined
    it 'should start a transaction', (done) ->
        dao.tx (err, result) ->
            should.not.exist err
            tx = result
            tx.should.be.an 'object'
            done()

    it 'transaction should enter row', (done) ->
        tx.insert
            table  : 'daobase_test'
            values : [2, 'another', 5]
            (err, result) ->
                should.not.exist err
                result.should.be.true
                done()

    it 'row should be visible for transaction', (done) ->
        tx.get
            table  : 'daobase_test'
            fields : 'id'
            values : 2
            (err, result) ->
                should.not.exist err
                result.id.should.equal 2
                result.string.should.equal 'another'
                result.number.should.equal 5
                done()

    it 'row should not be there outside transaction', (done) ->
        dao.get
            table  : 'daobase_test'
            fields : 'id'
            values : 2
            (err, result) ->
                err.should.be.truthy
                err.type.should.equal 'DAOBASE_GET_NO_MATCH'
                should.not.exist result
                done()

    it 'commit should work', (done) ->
        tx.commit (err) ->
            should.not.exist err
            done()

    it 'insert with specific values should work', (done) ->
        dao.insert
            table  : 'daobase_test'
            fields : ['id', 'number']
            values :
                number : 10
                id     : 3
            (err, result) ->
                should.not.exist err
                result.should.be.true
                dao.get
                    table  : 'daobase_test'
                    fields : 'id'
                    values : '3'
                    (err, result) ->
                        should.not.exist err
                        result.id.should.equal 3
                        should.not.exist result.string
                        result.number.should.equal 10
                        done()

    it 'insert with returning should work', (done) ->
        dao.insert
            table     : 'daobase_test'
            fields    : ['string', 'number']
            values    : ['test', 0]
            returning : 'id'
            (err, result) ->
                should.not.exist err
                result.should.be.an 'number'
                done()

    it 'getMulti should return whole table', (done) ->
        dao.getMulti
            table  : 'daobase_test'
            fields : []
            values : []
            (err, result) ->
                should.not.exist err
                result.length.should.equal 4
                for r in result
                    r.id.should.be.truthy
                    r.number.should.be.truthy
                    r.string.should.be.truthy
                done()

    it 'getMulti should return single row', (done) ->
        dao.getMulti
            table  : 'daobase_test'
            fields : ['id', 'number']
            values :
                id     : 3
                number : 10
            (err, result) ->
                should.not.exist err
                result.length.should.equal 1
                result[0].id.should.equal 3
                result[0].number.should.equal 10
                done()

    it 'delete should work', (done) ->
        dao.delete
            table  : 'daobase_test'
            fields : ['string', 'number']
            values : ['test', 0]
            (err, result) ->
                should.not.exist err
                result.should.equal 1
                done()

    it 'delete should not work when called again', (done) ->
        dao.delete
            table  : 'daobase_test'
            fields : ['string', 'number']
            values : ['test', 0]
            (err, result) ->
                err.should.be.truthy
                err.type.should.equal 'DAOBASE_DELETE_NO_MATCH'
                should.not.exist result
                done()