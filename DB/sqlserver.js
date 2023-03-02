module.exports =  class DBO {

     constructor({
            id,
            user,
            password,
            server,
            database,
            logSql ,
            maxRetryTime,
            waitBeforeRetry,
            maxQueryLength,
            convertResult,
        }){

        // this.mssql =  require('mssql/msnodesqlv8');
        this.mssql =  require("mssql");        
        this._opened = false;
        this._lastTryTime = null;

        this.id = id;
        this.logSql = logSql || false; 
        this.maxRetryTime = maxRetryTime || 60000; //Time in Milliseconds to retry opening connection If all connections are used 
        this.waitBeforeRetry = waitBeforeRetry || 1500; //milliseconds to wait before retrying to connect
        this.maxQueryLength = (typeof maxQueryLength == 'number')?maxQueryLength:32000;
        this.convertResult = convertResult || false;  //converts the result into standard format

        this.autoCloseTimer = null; //reference to timer
        this.connection = null; //reference to odbc connection object
        this.transaction = null;
        this.user = user;
        this.password = password;
        this.server = server;
        this.database = database;
        
     }

     async open() {   
        
        if(this.connection)
            return;
       
        this.connection = await this.mssql.connect({
            user: this.user,
            password: this.password,
            server: this.server,
            database: this.database,
            options: {
                enableArithAbort: false,
                encrypt: false,
            },
            pool: {
                min: 2,
                max: 10,
            },
            requestTimeout: this.waitBeforeRetry || 900000,
        });


}
  

     async close(){
        try{
            await this.connection.close();
            this.connection = null;
        }catch(ex){
            this.connection = null;
            throw ex;
        }
     }

     _wait(time){
        return new Promise(res => {setTimeout(res,time)})
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

        if(this.transaction){

            results = await this.transaction.sql(sql);

        } else{

            await this.tryOpen();

            if (this.logSql) {
                    console.log(this.id + ':', sql);
            }

            results = await this.connection.query(sql);

        }        

        results =  results.recordset;
        
        if(this.convertResult && results.length)
            results = this.standarizeResult(results)
        
        return results;
     }

     query(sql){ return this.sql(sql) }

     async startTransaction  () {

        if(this.transaction){

            throw  {
                code: 'TRANSACTION_IS_ALREADY_STARTED',
                message: 'A transaction is already running'
            }

        }
            
        let transaction = new this.mssql.Transaction(/* [pool] */);

        let startTrans = await transaction.begin(),
            request = new this.mssql.Request(startTrans);
        
        startTrans.sql = async (sql) => {  
            let results = await request.query(sql);
            return results;
        };
        
        this.transaction = startTrans;

        return startTrans;
        
       
    }

    async commit() {

        try{
            await this.transaction.commit();
            this.transaction = null;
        }catch(ex){
            this.transaction = null;
            throw ex;
        }       
    }

    async rollback() {

        try{
            await this.transaction.rollback();
            this.transaction = null;
        }catch(ex){
            this.transaction = null;
            throw ex;
        }     
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