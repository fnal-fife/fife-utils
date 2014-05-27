#!/usr/bin/env python
from samweb_client.utility import fileEnstoreChecksum
import os
import sys
from ifdh import ifdh

def checksum_dcache(file):
    fifo ="/%s/fifo_%d" % (os.environ.get("TMPDIR","/tmp"),os.getpid())
    os.mkfifo(fifo , 0600)
    
    try:
        pid = os.fork()

	if (0 == pid):

	    handle = ifdh()
	    res = handle.cp( [ file, fifo ] )
	    os.unlink(fifo)
	    os._exit(res)

	else:
	    res = fileEnstoreChecksum(fifo)
	    r2 = os.waitpid(pid,0)
	    #print "r2: " , r2
	    if r2[1] != 0:
		raise RuntimeError("ifdh cp failed")

    except:

       if os.access(fifo, os.R_OK):
          os.unlink(fifo)

       raise

    return res


if __name__ == "__main__":
    for name in sys.argv[1:]:
        print name, ": ", checksum_dcache(name)
