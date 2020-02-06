#!/usr/bin/env python3

# Copyright (C) 2012-2020
#
# * Volker Diels-Grabsch <v@njh.eu>
# * art0int <zvn_mail@mail.ru>
# * Matthew Fearnley (matthew.w.fearnley@gmail.com)
# * rhn <gihu.rhn@porcupinefactory.org>
#
# Permission to use, copy, modify, and/or distribute this software for any
# purpose with or without fee is hereby granted, provided that the above
# copyright notice and this permission notice appear in all copies.
#
# THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES
# WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
# MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR
# ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
# WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
# ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
# OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.

import hashlib
import os
import os.path
import struct
import sys
import threading


(remote_size, blocksize, offset_blocks, filename_len, hashname_len) = struct.unpack('<QQQQQ', sys.stdin.buffer.read(8+8+8+8+8))
filename = sys.stdin.buffer.read(filename_len)
hashname = sys.stdin.buffer.read(hashname_len).decode('utf-8')

sanity_hash = hashlib.new(hashname, filename).digest()
sys.stdout.buffer.write(sanity_hash)

f = open(filename, 'rb')
try:
    f.seek(0, os.SEEK_END)
    size = f.tell()
finally:
    f.close()
sys.stdout.buffer.write(struct.pack('<Q', size))

sys.stdout.flush()

if sys.stdin.buffer.read(2) != b'go':
    sys.exit()

hashes = []

def send_digests():
    hash_total = hashlib.new(hashname)
    with open(filename, 'rb') as f:
        readremain = size - offset_blocks * blocksize
        rblocksize = blocksize
        f.seek(offset_blocks * blocksize, os.SEEK_SET)
        while True:
            if readremain <= blocksize:
                rblocksize = readremain
            block = f.read(rblocksize)
            if len(block) != rblocksize:
                with send_packet:
                    sys.stdout.buffer.write(b'x')
                    sys.stdout.buffer.flush()
                break # something went wrong
            hash_total.update(block)
            hashes.append(hash_total.digest())
            digest = hashlib.new(hashname, block).digest()
            with send_packet:
                sys.stdout.buffer.write(b'd')
                sys.stdout.buffer.write(digest)
                sys.stdout.buffer.flush()
            readremain -= rblocksize
            if readremain == 0:
                break
    with send_packet:
        sys.stdout.buffer.write(b't')
        sys.stdout.buffer.write(hash_total.digest())
        sys.stdout.buffer.flush()


def handle_data():
    with open(filename, 'rb') as f:
        while True:
            pkt = sys.stdin.buffer.read(4)
            if pkt == b'send':
                (block_idx,) = struct.unpack('<Q', sys.stdin.buffer.read(8))
                f.seek(block_idx * blocksize)
                read_size = min(remote_size - block_idx * blocksize, blocksize)
                block = f.read(read_size)
                with send_packet:
                    sys.stdout.buffer.write(b'b')
                    sys.stdout.buffer.write(block)
                    sys.stdout.buffer.write(hashes[block_idx - offset_blocks])
                sys.stdout.buffer.flush()
            elif pkt == b'':
                break
            else:
                raise RuntimeError("Bad packet received: {}", pkt)

send_packet = threading.Lock()

datas = threading.Thread(target=handle_data)
datas.start()
send_digests()
datas.join()

