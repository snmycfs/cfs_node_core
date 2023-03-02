(async () => {

    const { exec } = require('child_process');

    const fs = require('fs')

    try{
        fs.unlinkSync('package-lock.json')
    }catch(ex){}

    exec(`npm uninstall odbc `)

})();