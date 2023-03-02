const DB = require('./DB');

const {SqlServerORM,JsonFileDbORM,ProgressORM} = require('orm');

const moment = require('moment');

module.exports = {
    DB,
    moment,
    SqlServerORM,JsonFileDbORM,ProgressORM,
}