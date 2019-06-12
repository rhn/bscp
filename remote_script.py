import hashlib
import os
import os.path
import struct
import sys

(remote_size, blocksize, filename_len, hashname_len) = struct.unpack('<QQQQ', sys.stdin.read(8+8+8+8))
filename = sys.stdin.read(filename_len)
hashname = sys.stdin.read(hashname_len)

sanity_hash = hashlib.new(hashname, filename).digest()
sys.stdout.write(sanity_hash)

f = open(filename, 'rb')
try:
    f.seek(0, 2)
    size = f.tell()
finally:
    f.close()
sys.stdout.write(struct.pack('<Q', size))

sys.stdout.flush()

if sys.stdin.read(2) != 'go':
    sys.exit()

hash_total = hashlib.new(hashname)

f = open(filename, 'rb')
try:
    f.seek(0, 2)
    readremain = size
    rblocksize = blocksize
    f.seek(0)
    while True:
        if readremain <= blocksize:
            rblocksize = readremain
        block = f.read(rblocksize)
        if len(block) != rblocksize:
            break
        hash_total.update(block)
        digest = hashlib.new(hashname, block).digest()
        sys.stdout.write('d')
        sys.stdout.write(digest)
        sys.stdout.flush()
        if sys.stdin.read(4) == 'send':
            sys.stdout.write('b')
            sys.stdout.write(block)
            sys.stdout.flush()
        readremain -= rblocksize
        if readremain == 0:
            break
finally:
    f.close()

sys.stdout.write('t')
sys.stdout.write(hash_total.digest())
sys.stdout.flush()

end
'''
    while True:
        position_s = sys.stdin.read(8)
        if len(position_s) == 0:
            break
        (position,) = struct.unpack('<Q', position_s)
        block = sys.stdin.read(blocksize)
        f.seek(position)
        f.write(block)
    readremain = size
    rblocksize = blocksize
    hash_total = hashlib.new(hashname)
    f.seek(0)
    while True:
        if readremain <= blocksize:
            rblocksize = readremain
        block = f.read(rblocksize)
        if len(block) == 0:
            break
        hash_total.update(block)
        readremain -= rblocksize
        if readremain == 0:
	    break
sys.stdout.write(hash_total.digest())'''
