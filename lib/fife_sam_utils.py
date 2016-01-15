#!/usr/bin/env python

import datetime
import grp
import optparse
import os
import re
import socket
import subprocess
import sys
import time
import traceback

import ifdh
from samweb_client import *

try:
  import hashlib 
except:
  import md5
  class hashlib:
     pass
  hashlib.md5 = md5.md5

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
        def __init__(self, locmap, fulllocflag = False):
            #print "in _loc_iterator.__init__, locmap is:", locmap
            self.loc_iter = [].__iter__()
            self.locmap = locmap
            self.fulllocflag = fulllocflag
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
            if not self.fulllocflag:
                prefix, res = res.split(':',1)
            res = re.sub('\(.*?\)$','',res)

            #print "converted\n\t%s\nto\n\t%s" % ( pre, res)

            return res + '/' + self.curfile

    def fullpath_iterator(self, fulllocflag = False):
        flist = self.get_flist()
        locmap = {}
        while len(flist) > 0:
            first_k = flist[:500]
            flist = flist[500:]
            locmap.update(self.ifdh_handle.locateFiles(first_k))
        return self._loc_iterator(locmap, fulllocflag)

def sampath(dir):
    if dir.find("s3:") == 0:
       return dir

    if dir.find("://") > 0:
        return dir

    return dir[dir.find(":")+1:]
    
def samprefix(dir):

    #print "entering samprefix(%s)" % dir
   
    if dir.find("s3:") == 0:
        return ""

    if dir.find("://") > 0:
        # it is a URL, so leave it alone!
        return ""
    #
    # try data disks
    #
    try:
        nowhere=open("/dev/null","w")
	l = subprocess.Popen(['samweb', '-e', os.environ['EXPERIMENT'], 'list-data-disks' ], stdout=subprocess.PIPE, stderr=nowhere).stdout.readlines()
        nowhere.close()
	for pp in l:
           pp = pp.strip('\n')
	   prefix, rest = pp.split(":",1)
           #print "checking:", pp, "->",  prefix,  ":", rest
	   if dir.startswith(rest):
		return "%s:" % prefix
    except:
        print "exception in samweb list-data-disks..."
        pass

    if (dir.startswith('/pnfs/uboone/scratch')):
       return 'fnal-dcache:'

    elif (dir.startswith('/pnfs/%s/scratch' % os.environ.get('EXPERIMENT'))):
       return 'dcache:'

    elif (dir.startswith('/pnfs/%s/persistent' % os.environ.get('EXPERIMENT'))):
       return 'dcache:'

    elif (dir.startswith('/pnfs')) :
       return 'enstore:'

    elif (dir.startswith('/grid/') or dir.startswith('/%s/'%os.environ.get('EXPERIMENT',None))):
       print "saw grid or experiment..."
       if (os.environ.get('EXPERIMENT') in ['minerva',]):
           return os.environ.get('EXPERIMENT') + '_bluearc:'
       else:
           return os.environ.get('EXPERIMENT') + 'data:'

    else:
       return socket.gethostname() + ':'

def basename(path):
    return path[path.rfind('/')+1:]

def dirname(dir):
    l = dir.rfind('/', 0,len(dir)-2 )
    return dir[0:l]

def canonical(uri):
    # get rid of doulbed slashes past the protocol:// part
    # and /dir/./path

    if uri.startswith('s3://'):
       uri = uri[0:3]+uri[4:]

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



def check_destination(samweb,dest):
    # /pnfs is officially okay
    if dest.find('/pnfs/') == 0:
        return 1
    samdest = samprefix(dest) + sampath(dest)
    # or any known data disk..
    #print "samdest is:" , samdest
    for d in samweb.listDataDisks():
        pos = samdest.find(d['mount_point'])
        #print "d is: " , d
        #print "d.mount_point %s pos: %d" % (d['mount_point'], pos)
        if samdest.find(d['mount_point']) == 0:
            return 1
    # otherwise, nope.
    return 0

def zerodeep(f):
    return '/'

def onedeep(f):
    h = hashlib.md5(f).hexdigest()
    return "/%s/" % h[0:4]

def twodeep(f):
    h = hashlib.md5(f).hexdigest()
    return "/%s/%s/" % (h[0:4], h[4:8])

def threedeep(f):
    h = hashlib.md5(f).hexdigest()
    return "/%s/%s/%s/" % (h[0:4], h[4:8], h[8:12])

def fourdeep(f):
    h = hashlib.md5(f).hexdigest()
    return "/%s/%s/%s/%s/" % (h[0:4], h[4:8], h[8:12], h[12:16])

notmade = {}
def dodir(ih, dir):
    
    if dir.startswith("/pnfs/") or dir.startswith("/%s/"%os.environ.get('EXPERIMENT','')):
        # we think globus-url-copy with -cd will make these, so we're done
        return

    for d in (dirname(dir), dir):
        if notmade.get(d,True):
           notmade[d] = False

           if d.startswith('s3:/') and not d.startswith('s3://'):
               d = 's3://' + d[4:]

           try:
               #print "doing mkdir " , d
               ih.mkdir(d, '')
           except:
               pass

def already_there( f, loclist, dest ):
    res = False
    dest = dest + '/'
    for p in loclist:
	if p.find(dest) != -1:
	    print "Notice: file %s already has a copy at %s, skipping" % ( f, p)
	    res = True
            break
    return res

def copy_and_declare(d, cpargs, locargs, dest, subdirf, samweb, just_say, verbose, paranoid ):
    res = 0

    if len(cpargs) == 0:
        # nothing to do!
        return res

    # trailing ; confuses ifdh cp
    if cpargs[-1] == ';':
        cpargs = cpargs[:-1]

    if just_say:
	print "I would 'ifdh cp %s'" % cpargs
	for f in locargs:
	    print "I would declare location for %s of %s" % (f, dest+subdirf(f))
    else: 
        if verbose: print "doing ifdh cp %s" % cpargs
	res = d.ifdh_handle.cp(cpargs)
        if verbose: print "ifdh cp returns %d" % res
        # XXX note this is arguably incorrect, we only declare locations if
        #     *all* the copies in the batch succeed; but if *some* of them
        #     do we don't...
	if res == 0:
	    if verbose: print "doing locargs: ", locargs
	    for f in locargs:
                try:
                    if paranoid:
                         cpdest = f  
                         if cpdest.find("s3:/") == 0 and cpdest.find("s3://") != 0:
                            cpdest="s3://"+cpdest[4:]
                         if len(d.ifdh_handle.ls(cpdest,1,'')) == 0:
                            print "unable to verify copy to ", cpdest, " not declaring."
                            continue

                    loc =  samprefix(dest) + dest + subdirf(f)
                    if verbose: print "addFileLocation(%s, %s)" % (f, loc)
		    samweb.addFileLocation(f, loc )
                except:
                    raise
                    res = 1
    return res

def clone( d, dest, subdirf = twodeep, just_say=False, batch_size = 20, verbose = False, experiment = None, ncopies=1, just_start_project = False, connect_project = False , projname = None, paranoid = False):
    # make gridftp tool add directories
    os.environ['IFDH_GRIDFTP_EXTRA'] = '-cd -sync'
    cpargs = []
    locargs = []
    count = 0
    samweb = SAMWebClient(experiment = experiment)

    if not check_destination(samweb,dest):
        print "Destination: %s is not known to SAM" % dest;
        print "...maybe you wanted ifdh_fetch?"
        return 1

    res = samweb.listApplications(name="sam_clone_dataset")

    #print 'here1'

    if not res:
        samweb.addApplication("fife_utils","sam_clone_dataset","1")
       
    #print 'here2'

    user = os.environ.get("GRID_USER", os.environ.get("USER","unknown"))

    if projname == None:
        projname = time.strftime("sam_clone_%%s_%Y%m%d%H_%%d")%(user,os.getpid())
    hostname = socket.gethostname()

    if connect_project:
        purl = d.ifdh_handle.findProject(projname, os.environ.get('SAM_STATION',experiment))
    else:
        purl = d.ifdh_handle.startProject(projname, os.environ.get('SAM_STATION',experiment), d.name, user, experiment)
        time.sleep(5)

    if verbose or just_start_project:
        print ("found" if connect_project else "started"), "project:", projname, "->", purl

    if (just_start_project):
        return 

    kidlist = []
    for i in range(0,int(ncopies) - 1):
        res = os.fork()
        if res > 0:
           kidlist.append(res)
           if verbose: print "started child", res
        if res == 0:
           # we are a child
           kidlist = []
           break
        if res < 0:
           print "Could not fork!"

    # rebuild our SAMWebClient and ifdh handle if we forked...
    # we've seen confusion in some cases, so just to be safe...
    if ncopies > 0:
        samweb = SAMWebClient(experiment = experiment)
        d.ifdh_handle = ifdh.ifdh()
       
    consumer_id = d.ifdh_handle.establishProcess( purl, "sam_clone_dataset", "1", hostname, user, "fife_utils", "sam_clone_dataset project", 0 , "")
    consumer_id = consumer_id.strip()
    if verbose:
         print "got consumer id: ", consumer_id

    if consumer_id == "":
         print "Error: could not establish sam consumer id for project: ", projname
         sys.exit(1)

    furi = d.ifdh_handle.getNextFile(purl, consumer_id)

    # deal with single/double s3 urls silliness..

    cpdest = dest
    if dest.find("s3:/") == 0 and dest.find("s3://") != 0:
        cpdest="s3://"+dest[4:]

    while furi:

	if verbose:
	    print "got file uri:", furi

        f = basename(furi)

        loclist = samweb.getFileAccessUrls(f, schema = "gsiftp")
        loclist = loclist + samweb.getFileAccessUrls(f, schema = "s3")

        if already_there(f, loclist, dest):
            d.ifdh_handle.updateFileStatus(purl, consumer_id,fname, 'transferred')
            d.ifdh_handle.updateFileStatus(purl, consumer_id,fname, 'consumed')
            continue
                
        if len(loclist) > 0:
            locargs.append(f)
            cpargs.append(loclist[0])
	    cpargs.append(cpdest + subdirf(f) + f)
            cpargs.append(';')
            dodir(d.ifdh_handle, dest+subdirf(f))
        else:
            print "Notice: skipping file %s, no locations" % f

        count = count + 1
        if count % batch_size == 0:

	    # above loop leaves a trailng ';', which confuses things.
            copy_and_declare(d, cpargs, locargs, dest, subdirf, samweb, just_say, verbose, paranoid)
            cpargs = []
            locargs = []

        d.ifdh_handle.updateFileStatus(purl, consumer_id,f, 'transferred')
        d.ifdh_handle.updateFileStatus(purl, consumer_id,f, 'consumed')
        furi = d.ifdh_handle.getNextFile(purl, consumer_id)

    copy_and_declare(d, cpargs, locargs, dest, subdirf, samweb, just_say, verbose, paranoid)
    d.ifdh_handle.setStatus( purl, consumer_id, "completed")

    if len(kidlist) > 1:
       # we're the parent, and there are kids
       for i in range(0,kidlist-1):
           os.wait()

    if len(kidlist) > 1 or int(ncopies) == 1:
       d.ifdh_handle.endProject(purl)


def unclone( d, just_say = False, delete_match = '.*', verbose = False, experiment = '', nparallel = 1 ):
    proccount = 0
    samweb = SAMWebClient(experiment = experiment)
    for full in d.fullpath_iterator(True):
        if verbose: print "looking at full path:", full
        spath = sampath(full)
        file = basename(full)
        if just_say:
            if re.match(delete_match, full) or re.match(delete_match, spath):
                print "I would 'ifdh rm %s'" % full
                print "I would remove location %s for %s" % ( file, samprefix(full)+dirname(full) )
        else:
            if re.match(delete_match, full) or re.match(delete_match, spath):
                if verbose: print "matches: " , delete_match
                if len(d.ifdh_handle.locateFile(file)) == 1:
                    print "NOT removing %s, it is the only location!"
                    continue
                try:
                    if verbose: print "removing: " , full
                    if full.find("s3:/") == 0:
                       path = full[0:4]+full[3:]
                    else:
                       path = sampath(full)

                    # unlink in background, wait if we
                    # have nparallel ones running.

                    pid = os.fork()
                    if 0 == pid:
                        d.ifdh_handle = ifdh.ifdh()
                        d.ifdh_handle.rm(path, '')
                        os.exit(0)
                    elif -1 == pid:
                        print "Cannot fork!"
                        d.ifdh_handle.rm(path, '')
                    else: 
                        proccount = proccount + 1
                        while proccount >= nparallel:
                           os.wait()
                           proccount = proccount - 1
                except:
                    traceback.print_stack()
                    pass
                loc = dirname(full)
                if verbose: print "removing location: " , loc , " for " , file
                try:
                    samweb.removeFileLocation(file, loc)
                except:
                    traceback.print_stack()
                    pass
            else:
                print "not matches: " , delete_match

    # clean up rm threads
    while proccount > 0:
       os.wait()
       proccount = proccount - 1

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
        for d  in [ 's3:/nova-analysis/data/output/stuff', '/pnfs/%s/raw/' % exp, '/pnfs/%s/scratch' % exp , '/%s/data/' % exp, 'srm://smuosge.smu.edu/foo/bar?SFN=/data/%s/file' % exp, 'gsiftp://random.host/stuff/%s/file' % exp ]:
           print d , "->", sampath(d), '//', samprefix(d)
