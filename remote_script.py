import hashlib
import os
import os.path
import struct
import sys

(remote_size, blocksize, filename_len, hashname_len) = struct.unpack('<QQQQ', sys.stdin.buffer.read(8+8+8+8))
filename = sys.stdin.read(filename_len)
hashname = sys.stdin.read(hashname_len)

sanity_hash = hashlib.new(hashname, filename.encode('utf-8')).digest()
sys.stdout.buffer.write(sanity_hash)

f = open(filename, 'rb')
try:
    f.seek(0, 2)
    size = f.tell()
finally:
    f.close()
sys.stdout.buffer.write(struct.pack('<Q', size))

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
        sys.stdout.buffer.write(b'd')
        sys.stdout.buffer.write(digest)
        sys.stdout.flush()
        if sys.stdin.read(4) == 'send':
            sys.stdout.buffer.write(b'b')
            sys.stdout.buffer.write(block)
            sys.stdout.flush()
        readremain -= rblocksize
        if readremain == 0:
            break
finally:
    f.close()

sys.stdout.buffer.write(b't')
sys.stdout.buffer.write(hash_total.digest())
sys.stdout.flush()

