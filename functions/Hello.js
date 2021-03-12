const execSync = require('child_process').execSync;
function main(params) {
    execSync("printf '\x00\x00\x00\x01' | sudo dd bs=8 status=none of=/dev/pqii_pci count=1 seek=21");
    var name = params.name || 'World';
    res = {payload: 'Hello, ' + name + '!'};
    execSync("printf '\x00\x00\x00\x02' | sudo dd bs=8 status=none of=/dev/pqii_pci count=1 seek=21");
    return res;
}
