chai = require 'chai'
should = chai.should()

dbUrl = 'postgres://postgres:postgres@localhost/test'

describe 'daobase', ->
    dao = require './dao'
    dao = new dao(dbUrl)

    it 'should create a table', (done) ->
        query =
            'CREATE TABLE daobase_test (
               id serial,
               string text,
               number integer
             );'
        dao.sqlOp [query], (err, result) ->
            should.not.exist err
            done()

    it 'get should not find a row', (done) ->
        dao.get
            table : 'daobase_test'
            field : 'id'
            value : 1
            (err, result) ->
                err.should.be.truthy
                err.type.should.equal 'DAOBASE_GET_ROW_NOT_FOUND'
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
            table : 'daobase_test'
            field : 'id'
            value : 1
            (err, result) ->
                should.not.exist err
                result.id.should.equal 1
                result.string.should.equal 'test'
                result.number.should.equal 0
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
            table : 'daobase_test'
            field : 'id'
            value : 2
            (err, result) ->
                should.not.exist err
                result.id.should.equal 2
                result.string.should.equal 'another'
                result.number.should.equal 5
                done()

    it 'row should not be there outside transaction', (done) ->
        dao.get
            table : 'daobase_test'
            field : 'id'
            value : 2
            (err, result) ->
                err.should.be.truthy
                err.type.should.equal 'DAOBASE_GET_ROW_NOT_FOUND'
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
            values : [3, 10]
            (err, result) ->
                should.not.exist err
                result.should.be.true
                dao.get
                    table : 'daobase_test'
                    field : 'id'
                    value : '3'
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