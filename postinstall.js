(async () => {


    const { exec } = require('child_process');
  
    
    let majorVersion = parseInt(process.versions.node.split('.')[0]);

    let isLinux = process.platform.startsWith('linux')?true:false;

    let modulesToInstall = [];

    if(majorVersion < 9){

        if(isLinux && majorVersion == 8)
            modulesToInstall.push('custom-odbc-1.2.1.tgz') //installing pre-compiled binary
        else
            modulesToInstall.push('odbc@1.2.1')

    } else if(majorVersion < 12){
        modulesToInstall.push('odbc@2.3.5')
    }        
    else{
        modulesToInstall.push('odbc')
    }

    // try{
    //     fs.unlinkSync('package-lock.json')
    // }catch(ex){}
    
   

    exec(`npm install ${modulesToInstall.join(' ')} --no-save --unsafe-perm`)

})();