module.exports =  class DBO {

     constructor({
            id,
            connString,
            logSql ,
            maxRetryTime,
            waitBeforeRetry,
            maxQueryLength,
            autoCloseTimeout,
            convertResult,
        }){

        this.odbc = require('odbc'); //for node  > ver 8         
        this._opened = false;
        this._lastTryTime = null;

        this.id = id;
        this.connString = connString;
        this.logSql = logSql || false; 
        this.maxRetryTime = maxRetryTime || 60000; //Time in Milliseconds to retry opening connection If all connections are used 
        this.waitBeforeRetry = waitBeforeRetry || 1500; //milliseconds to wait before retrying to connect
        this.maxQueryLength = (typeof maxQueryLength == 'number')?maxQueryLength:32000;
        this.autoCloseTimeout = autoCloseTimeout || 800; //milliseconds to wait before automatically closing connection
        this.convertResult = convertResult || true;  //converts the result into standard format

        this.autoCloseTimer = null; //reference to timer
        this.connection = null; //reference to odbc connection object
     }

     open(){
        return new Promise((res,rej) => {
             if(this._opened) return res();

             this.odbc.connect(this.connString,(err,connection) => {

                 if(err)
                    return rej(err);
                this._opened = true;    
                this.connection = connection;            
                res(connection)
             });
        })
     }

     close(){
        return new Promise((res,rej) => {
            if(!this._opened) return res();

             this.connection.close(err => {

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
             this.connection.query(sql, (err,rows,hasMore) => {

                if(err){
                    if(typeof err !== 'object')
                        err = {code:'SQL_ERROR',message:err}
                    
                    err.sql = sql;
                        
                    return rej(err);
                 }
                
                res(rows)
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
      
        results = await this._getData(sql);
        
        if(this.convertResult && results.length)
            results = this.standarizeResult(results)
        
        this.autoCloseTimer = this._setAutoCloseTimer();
        return results;
     }

     query(sql){ return this.sql(sql) }

     async startTransaction  () {
        
        return new Promise((res,rej) => {

            this.connection.beginTransaction(err => {
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

            this.connection.commit(err => {
                if(err){

                    if(this.logSql)
                        console.log(`${this.id}: Transaction Rolledback. Failure to commit`);

                    return rej(err);
                }                    
               
                if(this.logSql)
                    console.log(`${this.id}: Transaction Committed`);
                    

                res();
            });

        })
    }

    async rollback() {

        return new Promise((res,rej) => {

            this.connection.rollback(err => {
                if(err){
                    if(this.logSql)
                        console.log(`${this.id}: Transaction Rolledback. Failure to rollback`);
                    return rej(err);
                }                    
               
                if(this.logSql)
                    console.log(`${this.id}: Transaction Rolledback`);
                    

                res();
            });

        })
    }

    standarizeResult(data){
        let colsToConvert = [];
        for(let col of data.columns){
            switch(col.dataType){
                case -7: //means it is bit field
                colsToConvert.push(col);
                break;
                case -5: //means it is BIG INT
                colsToConvert.push(col);
                break;
            }
        }
        if(colsToConvert.length){
            for(let rec of data){
                for(let col of colsToConvert){
                    switch(col.dataType){
                        case -7: //means it is bit field
                            rec[col.name] = [true,1,'1','true'].includes(rec[col.name])?true:false;
                        break;   
                        case -5: //means it is BIG int field
                            rec[col.name] = parseInt(rec[col.name]);
                        break;                     
                    }
                   
                }
            }
        }

        return data;
    }

}