#!/usr/bin/env python
import sys
import os
from samweb_client import *


pnfs_prefixes = {
   "/pnfs/cdfen": "srm://cdfdca1.fnal.gov:8443/srm/managerv2?SFN=",
   "/pnfs/dzero": "srm://d0ca1.fnal.gov:8443/srm/managerv2?SFN=",
   "/pnfs/": "srm://fndca1.fnal.gov:8443/srm/managerv2?SFN=",
}

def get_prefix( path ) :
    list = pnfs_prefixes.keys()
    list.sort(reverse=1)
    for prefix in list:
       if path.startswith(prefix):
           return pnfs_prefixes[prefix]
    raise LookupError("no server known for %s" % path)

def get_pnfs_1_adler32_and_size( path ):
    sum = 0
    first = True
    cmd = "srmls -2 -l %s/pnfs/fnal.gov/usr/%s" % ( get_prefix(path), path[5:])
    #print "running: " , cmd
    pf = os.popen(cmd)
    for line in pf:
        #print "read: " , line
        if first:
            if line[-1] == '/' or line[-2] == '/':
                pf.close()
                raise LookupError('path is a directory: %s' % path)
            size = long(line[2:line.find('/')-1])
            first = False
            continue

        if line.find("Checksum value:") > 0:
            ssum = line[line.find(':') + 2:]
            #print "got string: ", ssum
            sum = long( ssum , base = 16 )
            #print "got val: %lx" % sum
            pf.close()
            return  sum, size

    pf.close()
    raise LookupError("no checksum found for %s" % path)

BASE = 65521

def convert_0_adler32_to_1_adler32(crc, filesize):
    crc = long(crc)
    filesize = long(filesize)
    size = int(filesize % BASE)
    s1 = (crc & 0xffff)
    s2 = ((crc >> 16) &  0xffff)
    s1 = (s1 + 1) % BASE
    s2 = (size + s2) % BASE
    return (s2 << 16) + s1

def pnfs_verify_checksum(path):
    samweb = SAMWebClient()
    filename = path[path.rfind("/") + 1:]
    meta = samweb.getMetadata(filename)
    #print meta
    #print "meta[checksum]:", meta['checksum']
    print "meta[checksum][8:]:", meta['checksum'][0][8:]
    print "meta[file_size]: ", meta['file_size']
    meta_adler32_0 = long(meta['checksum'][0][8:], base=10)
    meta_size = long(meta['file_size'])
    pnfs_adler32_1, pnfs_size = get_pnfs_1_adler32_and_size( path )
    meta_adler32_1 = convert_0_adler32_to_1_adler32(meta_adler32_0, meta_size)
    #print "meta: " , meta_size, ", ", "%lx" % meta_adler32_1, "<=",  "%lx" % meta_adler32_0
    #print "pnfs: " , pnfs_size, ", ", "%lx" % pnfs_adler32_1
    if meta_adler32_1 == pnfs_adler32_1 and meta_size == pnfs_size:
        return 1
    else:
        return 0


if __name__ == "__main__":
    ok_map = { 0: "Fail", 1: "Ok" }
    for path in sys.argv[1:]:
        print path, ": ", ok_map[pnfs_verify_checksum(path)]
