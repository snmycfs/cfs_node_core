class Transaction {

    constructor(dbo, agentId) {
        this.dbo = dbo;
        this.agentId = agentId;
        this.dbagent = dbo.dbobjs[agentId];
        this.orm = dbo.orm;

        this._logs = [];
        this._errorMsg = '';
    }


    async sql(sql) {
        return this.dbagent.sql(sql);
    }

    log(sql) {
        this._logs.push(sql)
    }

   
    async commit(runLogSqlsAfterCommit = false) {

        this._errorMsg = 'Cannot use transaction object after it is comitted'
        try {

            if (!runLogSqlsAfterCommit) {
                for (let log of this._logs) {
                    await this.sql(log.sql, log.dbOptions)
                }
                this._logs = [];
            }

            await this.dbagent.commit();

            if (runLogSqlsAfterCommit) {


                for (let sql of this._logs) {
                    await this.dbagent.sql(sql)
                }
                this._logs = [];


            }
           this.dbo.freeAgent(this.agentId)

        } catch (ex) {

            this._logs = [];
           this.dbo.freeAgent(this.agentId)
            throw ex;

        }


    }

    async rollback() {
        this._logs = [];
        this._errorMsg = 'Cannot use transaction object after it is rolled back'
        //rollback callback
        try {

           await this.dbagent.rollback();
           this.dbo.freeAgent(this.agentId)

        } catch (ex) {

           this.dbo.freeAgent(this.agentId)
           throw ex;

        }

    }

      //  ========= ORM wrapper methods ===============
    readOne(tableName, query, schema)  {
        return this.orm.readOne(this.dbagent,tableName,query,schema)
    }

    read(tableName, query, limit, schema)  {
        return this.orm.read(this.dbagent,tableName,query,limit,schema)
    }

    insert(tableName, params,schema){
        return this.orm.insert(this.dbagent,tableName, params,schema)
    }

    update(tableName, params,query, schema){
        return this.orm.update(this.dbagent,tableName, params, query, schema)
    }

    remove(tableName, query, schema){
        return this.orm.remove(this.dbagent,tableName,  query, schema)
    }

    getSchema(schema){
        return this.orm.getSchema(schema)
    }

    escape(str){
        return this.orm.escape(str)
    }

    getNextSeq(seqName){
        return this.orm.escape(this.dbagent,seqName)
    }

    //  ========= END ORM wrapper methods ===============

}

module.exports = class DB {
    constructor(params) {
        return this._init(params); //in Javascript you cannot define a construct as async except with this work around
    }

    async _init(params){
        let { id, connString, agents, childProcess, connector, databaseType } = params;
        
        this.id = id;
        this.connector = connector || 'odbc';
        this.connString = connString;
        this.databaseType = databaseType || 'sqlserver';

        this.dbobjs = {}; //asoociative array of workers
        this.agents = agents || 1; //ideally it should be num of connections available for broker
        //this._active = null;          

        this.freeAgents = [];
        this.queries = [];
        this.nodeMajorversion = parseInt(process.versions.node.split('.')[0]);

        this.orm = null;

        let DBO;
        if (this.connector.trim().toLowerCase() === 'sqlserver') {

            DBO = require('./sqlserver')           

        } else {
            if (this.nodeMajorversion < 9)
                DBO = require('./odbc1')
            else
                DBO = require('./odbc2')
        }


        let dboID = '';
        for (let i = 1; i <= this.agents; i++) {
            dboID = `${id}(${i})`;
            this.freeAgents.push(dboID);
            this.dbobjs[dboID] = new DBO({...params,id:dboID});
        }

        if(this.agents > 0)
           await this._initORM(params);
           
        return this;
    }

    query(query) { return this.sql(query) }

    log(query) { return this.sql(query) }

    sql(query) {
        return new Promise((res, rej) => {
            this.queries.push({ query, res, rej });
            this._next()
        })
    }

    getTransaction() {
        return new Promise((res, rej) => {
            this.queries.push({ startTrans: true, res, rej });
            this._next()
        })
    }

    freeAgent(agentId){
        this.freeAgents.push(agentId); //make agent free          
        return this._next()
    }


    _next() {
        if (!this.freeAgents.length || !this.queries.length)
            return;

        let agentId = this.freeAgents.shift();
        let params = this.queries.shift();
        this._execute(params, agentId)
        this._next();
    }

    async _initORM(params){
        if(this.orm)
            return this.orm;


        let opts = {
            dbName: params.id, //dbName is required by ORM
            ...params,
            dbo:this,           
        }

        
        const {SqlServerORM,ProgressORM} = require('orm'); //require('../ORM');

        if(this.databaseType === 'sqlserver'){

            this.orm = await new SqlServerORM(opts)

        }else  if(this.databaseType === 'progress')
            this.orm = await new ProgressORM(opts)
        else
             throw `Unfortunately database type ${this.databaseType} is NOT supported yet by cfs_node_core`

        return this.orm;
    }

    async _execute({ startTrans, query, res, rej }, agentId) {
        try {           

            if (startTrans) {

                await this.dbobjs[agentId].startTransaction()
                let trans = new Transaction(this)
                return res(trans);
            }

            let result = await this.dbobjs[agentId].sql(query)
            res(result)


        } catch (ex) {

            rej(ex);

        } finally {

            return this.freeAgent(agentId)
        }
    }
   

    //  ========= ORM wrapper methods ===============
    readOne(tableName, query, schema)  {
        return this.orm.readOne(this,tableName,query,schema)
    }

    read(tableName, query, limit, schema)  {
        return this.orm.read(this,tableName,query,limit,schema)
    }

    insert(tableName, params,schema){
        return this.orm.insert(this,tableName, params,schema)
    }

    update(tableName, params,query, schema){
        return this.orm.update(this,tableName, params, query, schema)
    }

    remove(tableName, query, schema){
        return this.orm.remove(this,tableName,  query, schema)
    }

    getSchema(schema){
        return this.orm.getSchema(schema)
    }

    escape(str){
        return this.orm.escape(str)
    }

    getNextSeq(seqName){
        return this.orm.escape(this,seqName)
    }

    //  ========= END ORM wrapper methods ===============

}
