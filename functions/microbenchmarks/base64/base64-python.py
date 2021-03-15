# Copyright (c) 2019 Princeton University
# Copyright (c) 2014 'Konstantin Makarchev'
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

import base64
import subprocess

def main(params):
    subprocess.run(["bash","-c",r"printf '\x00\x00\x00\x01' | sudo dd bs=8 status=none of=/dev/pqii_pci count=1 seek=21"])
    STR_SIZE = 1000000
    TRIES = 100
    str1 = b"a" * STR_SIZE
    str2 = b""
    s_encode = 0
    for _ in range(0, TRIES):
        str2 = base64.b64encode(str1)
        s_encode += len(str2)
    
    s_decode = 0
    for _ in range(0, TRIES):
        s_decode += len(base64.b64decode(str2))

    result = {'s_encode' : str(s_encode), 's_decode' : str(s_decode)}
    
    subprocess.run(["bash","-c",r"printf '\x00\x00\x00\x02' | sudo dd bs=8 status=none of=/dev/pqii_pci count=1 seek=21"])
    return result
