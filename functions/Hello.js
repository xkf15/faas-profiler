const execSync = require('child_process').execSync;
function main(params) {
    execSync('ls > /home/kaifengx/faas-profiler/out1');
    var name = params.name || 'World';
    return {payload: 'Hello, ' + name + '!'};
}
