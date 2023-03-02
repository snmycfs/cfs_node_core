module.exports =  class DBO {

     constructor({
            id,
            connString,
            logSql,
            maxRetryTime,
            waitBeforeRetry, 
            maxQueryLength,
            autoCloseTimeout,
        }){

        let odbcMod = 'odbc';
        this.isLinux = process.platform.startsWith('linux')?true:false;
        this.nodeMajorversion = parseInt(process.versions.node.split('.')[0]);

        if(this.isLinux && this.nodeMajorversion == 8)
            odbcMod = 'custom-odbc'

        this.odbc = require(odbcMod)(); //for node ver 8         
        this._opened = false;
        this._lastTryTime = null;

        this.id = id;
        this.connString = connString;
        this.logSql = logSql || false; 
        this.maxRetryTime = maxRetryTime || 60000; //Time in Milliseconds to retry opening connection If all connections are used 
        this.waitBeforeRetry = waitBeforeRetry || 1500; //milliseconds to wait before retrying to connect
        this.maxQueryLength = (typeof maxQueryLength == 'number')?maxQueryLength:32000;
        this.autoCloseTimeout = autoCloseTimeout || 800; //milliseconds to wait before automatically closing connection

        this.autoCloseTimer = null; //reference to timer
     }

     open(){
        return new Promise((res,rej) => {
             if(this._opened) return res();

             this.odbc.open(this.connString,err => {

                 if(err)
                    return rej(err);
                this._opened = true;                
                res()
             });
        })
     }

     close(){
        return new Promise((res,rej) => {
            if(!this._opened) return res();

             this.odbc.close(err => {

                 if(err)
                    return rej(err);

                this._opened = false;
                res()
             });
        })
     }

     _wait(time){
        return new Promise(res => {setTimeout(res,time)})
     }

     _setAutoCloseTimer(){
         return setTimeout(() => {
             this.close();
         },this.autoCloseTimeout)
     }

     async tryOpen(){
         let keepTrying = true;
         
         clearTimeout(this.autoCloseTimer); //cear timer first

         while(keepTrying){
            try{
              await this.open();
              keepTrying = false;
              this._lastTryTime = null;
            }catch(err){
                
                await this._wait(this.waitBeforeRetry)

                if(!this._lastTryTime)
                    this._lastTryTime = new Date().getTime();

                if ((new Date().getTime() - this._lastTryTime) > this.maxRetryTime) {
                    this._lastTryTime = null;
                    console.log("Retry failed:" + this.id);
                    err.code = 'DB_CONNECTION_ERROR';
                    throw err;
                }

                console.log('Retrying Opening Connection:' + this.id)
            }
         }
          
     }

     _getData(sql){
        return new Promise((res,rej) => {
             this.odbc.query(sql, (err,rows,hasMore) => {

                 if(err){
                    if(typeof err !== 'object')
                        err = {code:'SQL_ERROR',message:err}
                    
                    err.sql = sql;
                        
                    return rej(err);
                 }
                    
                
                res({rows,hasMore})
             });
        })
     }

     async sql(sql){

        if (this.logSql) {
            console.log(this.id + ':', sql);
        }

        let results = [], hasMore = true;

        if (this.maxQueryLength && sql.length >= this.maxQueryLength) {
            //It come to my knowledge there is a limitation of length of sql query in ODBC.                    
            throw {
                code: 'SQL_EXCEEDS_MAX_ALLOWED_LENGTH',
                message: 'Query is too long. Contact CFS.'
            }
        }

        await this.tryOpen();       

        while(hasMore){

            let batch = await this._getData(sql);

            if(batch.rows)
                results = [...results,...batch.rows]

            hasMore = batch.hasMore;    
        }        
        
        this.autoCloseTimer = this._setAutoCloseTimer();
        return results;
     }

     query(sql){ return this.sql(sql) }

     async startTransaction  () {
        
        return new Promise((res,rej) => {

            this.odbc.beginTransaction(err => {
                if(err)
                    return rej(err);
               
                if(this.logSql){
                    console.log(`${this.id}: Transaction started`);
                }                            
                res();
            });

        })
    }

    async commit() {

        return new Promise((res,rej) => {

            this.odbc.commitTransaction(err => {
                if(err){
                    console.log(`${this.id}: Transaction Rolledback. Failure to commit`);
                    return rej(err);
                }                    
               
                if(this.logSql){
                    console.log(`${this.id}: Transaction Committed`);
                }    

                res();
            });

        })
    }

    async rollback() {

        return new Promise((res,rej) => {

            this.odbc.rollbackTransaction(err => {
                if(err){
                    console.log(`${this.id}: Transaction Rolledback. Failure to rollback`);
                    return rej(err);
                }                    
               
                if(this.logSql){
                    console.log(`${this.id}: Transaction Rolledback`);
                }    

                res();
            });

        })
    }

}