#!/usr/bin/env python
import ifdh
import os
import re
import socket

class dataset:
    def __init__( self, name ):
        self.ifdh_handle = ifdh.ifdh()   
        self.name = name
        self.flist = None

    def get_flist( self ):
        if self.flist == None:
            self.flist =  self.ifdh_handle.translateConstraints("defname: %s " % self.name)
        return self.flist

    def file_iterator(self):
        flist = self.get_flist()
        return flist.__iter__()
        
    class _loc_iterator:
        def __init__(self, locmap):
            #print "in _loc_iterator.__init__, locmap is:", locmap
            self.loc_iter = [].__iter__()
            self.locmap = locmap
            self.key_iter = locmap.keys().__iter__()
            try:
                self.next_key()
            except StopIteration:
                pass

        def next_key(self):
            #print "in _loc_iterator.next_key..."
            self.curfile = self.key_iter.next()
 
            #print "curfile is: ",  self.curfile
            self.loc_iter = self.locmap[self.curfile].__iter__()

        def __iter__(self):
            return self
 
        def next(self):
            #print "in _loc_iterator.next..."
            res = None
            while res == None:
                try:
                    res = self.loc_iter.next()
                except StopIteration:
                    #print "in _loc_iterator.next, trying next key.."
                    # if *this* rasies StopIter, we bail...
                    self.next_key()

            #pre = res
            res = re.sub('^(enstore|[a-z]*data):','',res)
            res = re.sub('\(.*?\)$','',res)

            #print "converted\n\t%s\nto\n\t%s" % ( pre, res)

            return res + '/' + self.curfile

    def fullpath_iterator(self):
        locmap = dict(self.ifdh_handle.locateFiles(self.get_flist()))
        return self._loc_iterator(locmap)

def samprefix(dir):
    if (dir.startswith('/pnfs')) :
       return 'enstore:'
    elif (dir.startswith('/grid/') or dir.startswith('/%s/'%os.environ.get('EXPERIMENT',None))):
       return os.environ.get('EXPERIMENT') + 'data:'
    else:
       return socket.gethostname()

def basename(path):
    return path[path.rfind('/')+1:]

def dirname(dir):
    l = dir.rfind('/', 0,len(dir)-2 )
    return dir[0:l]

if __name__ == '__main__':
    os.environ['EXPERIMENT'] = 'nova'
    d1 = dataset('mwm_test_6')
    for f in d1.file_iterator():
        print "file: ", f
    print "-------------"
    for l in d1.fullpath_iterator():
        print "loc:" ,  l
