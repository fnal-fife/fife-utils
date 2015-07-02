#!/usr/bin/env python
import ifdh
import os
import re
import socket
import subprocess
import datetime

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
        flist = self.get_flist()
        locmap = {}
        while len(flist) > 0:
            first_k = flist[:500]
            flist = flist[500:]
            locmap.update(self.ifdh_handle.locateFiles(first_k))
        return self._loc_iterator(locmap)

def sampath(dir):
    if dir.find("://") > 0:
        # it is a URL, so convert it to a hostname/path
        path = re.sub("[a-z]+://([-a-z0-9_.]*)(/.*\?SFN=)?(/.*)","\\3", dir)
        return path
    return dir[dir.find(":")+1:]
    
def samprefix(dir):
    
    if dir.find("://") > 0:
        # it is a URL, so convert it to a hostname/path
        prefix = re.sub("[a-z]+://([-a-z0-9_.]*)(/.*\?SFN=)?(/.*)","\\1:", dir)
        return prefix
    #
    # try data disks
    #
    try:
        nowhere=open("/dev/null","w")
	l = subprocess.Popen(['samweb', '-e', os.environ['EXPERIMENT'], 'list-data-disks' ], stdout=subprocess.PIPE, stderr=nowhere).stdout.readlines()
        nowhere.close()
	for pp in l:
	   prefix, rest = pp.split(":",1)
	   if dir.startswith(pp):
		return "%s:" % prefix
    except:
        print "exception in samweb list-data-disks..."
        pass

    if (dir.startswith('/pnfs/uboone/scratch')):
       return 'fnal-dcache:'

    elif (dir.startswith('/pnfs/%s/scratch' % os.environ.get('EXPERIMENT'))):
       return 'dcache:'

    elif (dir.startswith('/pnfs')) :
       return 'enstore:'

    elif (dir.startswith('/grid/') or dir.startswith('/%s/'%os.environ.get('EXPERIMENT',None))):
       if (os.environ.get('EXPERIMENT') in ['minerva',]):
           return os.environ.get('EXPERIMENT') + '_bluearc:'
       else:
           return os.environ.get('EXPERIMENT') + 'data:'

    else:
       return socket.gethostname()

def basename(path):
    return path[path.rfind('/')+1:]

def dirname(dir):
    l = dir.rfind('/', 0,len(dir)-2 )
    return dir[0:l]

def canonical(uri):
    # get rid of doulbed slashes past the protocol:// part
    # and /dir/./path

    pos = uri.rfind('//')
    while pos > 7:
        uri = uri[:pos] + uri[(pos+1):]
        pos = uri.rfind('//')

    pos = uri.rfind('/./')
    while pos > 0:
        uri = uri[:pos] + uri[(pos+2):]
        pos = uri.rfind('/./')

    return uri      


def has_uuid_prefix(s):
    return bool(re.match(r'[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}',s))

if __name__ == '__main__':
    os.environ['EXPERIMENT'] = 'nova'
    #d1 = dataset('mwm_test_6')
    d1 = dataset('rock_onlyMC_FA141003xa_raw3')
    count1=0
    for f in d1.file_iterator():
        print "file: ", f
        count1=count1+1
    count2=0
    print "-------------"
    for l in d1.fullpath_iterator():
        print "loc:" ,  l
        count2 = count2+1
    print "count1 " , count1 , " count2 " , count2
    for exp in [ 'uboone', 'nova', 'minerva', 'hypot' ]:
        os.environ['EXPERIMENT'] = exp
        for d  in [ '/pnfs/%s/raw/' % exp, '/pnfs/%s/scratch' % exp , '/%s/data/' % exp, 'srm://smuosge.smu.edu/foo/bar?SFN=/data/%s/file' % exp, 'gsiftp://random.host/stuff/%s/file' % exp ]:
           print d , "->", samprefix(d)
