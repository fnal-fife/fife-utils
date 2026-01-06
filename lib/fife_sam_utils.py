#!/usr/bin/env python

try:
    from six import *
except:
    pass

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
import threading 
import logging
import ast
import requests

try:
    from data_dispatcher.api import DataDispatcherClient
except:
    class DataDispatcherClient:
        def __init__(self):
            raise NotImploementedError("need data_dispatcher dependency for this functionality")

try:
    from metacat.webapi import MetaCatClient
    import metacat.common.exceptions
except:
    class MetaCatClient:
        def __init__(self):
            raise NotImploementedError("need metacat dependency for this functionality")
    

try:
    from rucio.client.replicaclient import ReplicaClient
except:
    class ReplcaClient:
        def __init__(self):
            raise NotImploementedError("need rucio dependency for this functionality")
    
    

import ifdh
from samweb_client import *

try:
  import hashlib 
except:
  import md5
  class hashlib(object):
     pass
  hashlib.md5 = md5.md5

try:
   import urllib3
except:
   pass

try:
   urllib3.disable_warnings(category=urllib3.exceptions.InsecureRequestWarning)
except:
   pass

def check_dcache_queued():
    # this is turned off for now; need a token-authenticated way into
    # landscape data to check how busy dcache is...
    return None

def safe_getgrgid(n):
    try:
        return grp.getgrgid(n)
    except:
        return "unknown"

DCACHE_QUEUE_THRESHOLD = 200

def wait_for_dcache():
    dq = check_dcache_queued()
    while dq and dq > DCACHE_QUEUE_THRESHOLD:
        print("DCache is too busy: %d ftp transfers already queued.  Waiting 1 minute..." % dq)
        sys.stdout.flush()
        time.sleep(60)
        dq = check_dcache_queued()
        
            
def setup_environ(experiment = None):
    os.environ['EXPERIMENT'] = experiment
    os.environ['SAM_EXPERIMENT'] = experiment
    cert = get_standard_certificate_path({})
    os.environ['X509_USER_PROXY'] = cert

def log_startup():
    ih = ifdh.ifdh()
    ih.log("Starting: %s" % " ".join(sys.argv))
    

def log_finish(success):
    ih = ifdh.ifdh()
    ih.log("%s: %s" % (success ," ".join(sys.argv)))

def do_getawscreds(debug = False):
    ih = ifdh.ifdh()
    uname = os.environ.get('GRID_USER',os.environ.get('USER','unknown'))
    fname= ih.localPath("awst")
    open(fname,"w").close()
    os.chmod(fname,0o600)
    fname=ih.fetchInput("/pnfs/%s/scratch/users/%s/awst" % (
       os.environ['EXPERIMENT'],
       uname
    ))
    f = open(fname,"r")
    for line in f:
        var,val= line.strip().split('=',1)
        var = var.replace("export ","")
        val=val.strip('"')
        os.environ[var] = val
        if debug: print("Setting os.environ[%s]=%s" % (var, val))
    f.close()
    os.unlink(fname)

class fake_project_dataset:

    def __init__( self, name ):
        self.ifdh_handle = ifdh.ifdh()   
        self.name = name
        self.dims = "defname:%s" % name
        self.have_pnfs = os.access("/pnfs/", os.R_OK)
        self.flist = None
        self.locmap = None

    def get_flist( self ):
        if self.flist == None:
            self.flist =  self.ifdh_handle.translateConstraints(self.dims)
        return self.flist

    def file_iterator(self):
        flist = self.get_flist()
        return flist.__iter__()

    def get_locmap(self, fulllocflag = False):
        if self.locmap != None:
            return
        #print "get_locmap: starting", time.ctime()
        flist = self.get_flist()
        self.locmap = {}
        while len(flist) > 0:
            first_k = flist[:500]
            flist = flist[500:]
            self.locmap.update(self.ifdh_handle.locateFiles(first_k))
        #print "get_locmap: finishing", time.ctime()

    def fullpath_iterator(self, fulllocflag = False, tapeset = None):
        self.get_locmap(fulllocflag)
        return dataset._loc_iterator(self.locmap, fulllocflag, tapeset = tapeset)

    def location_has_file(self,fullpath):
        # optimize location checks for pnfs if mounted
        if fullpath[:6] == '/pnfs/' and self.have_pnfs and os.access(fullpath, os.R_OK):
            return 1
        res = self.ifdh_handle.ls(fullpath,1,'')
        return len(res) != 0

    def uncache_location(self,fp):
        pass

    def get_paths_for(self,filename):
        loclist = self.ifdh_handle.locateFile( filename )
        loclist = [ sampath(loc) + '/' + filename  for loc in loclist]
        return loclist
         
    def remove_path_for(filename, fp):
        pass

    def startProject(self, projname, station, dataset, user, experiment):
        return "fake://fake"

    def findProject(self, projname, station):
        return "fake://fake"

    def establishProcess(self,  purl, a, vers, hostname, user, pkg, desc, lim , schema):
        self.count = 0
        self.fakeproc = self.file_iterator()
        return "1"

    def updateFileStatus(self, purl, consumer_id,f, status):
        return 1

    def getNextFile(self, purl, consumer_id):
        try:
            return self.fakeproc.next()
        except:
            return ""
      
    def endProject(self, purl):
        return 1

    def wrap_ls(self, path, n, force):
        res = self.ifdh_handle.ls(path,n,force)
        return res


class fake_file_dataset:

    def __init__( self, filename ):
        self.ifdh_handle = ifdh.ifdh()   
        self.filename = filename
        self.name = 'file %s' % filename
        self.dims = "file %s" % filename
        self.flist = [ filename ]
        self.have_pnfs = os.access("/pnfs/", os.R_OK)

    def file_iterator(self):
        flist = [ self.filename ]
        return flist.__iter__()

    def fullpath_iterator(self, fulllocflag = False, tapeset = None):
        locs = self.ifdh_handle.locateFile( self.filename )
        loclist = []
        for l in locs:
            if l.find('(') > 0:
                if tapeset != None:
                    tapeset.add(l[l.find('@')+1:-1])
                l = l[:l.find('(')]
            if l.find(':') > 0:
                l = l[l.find(':')+1:]

            loclist.append(l + '/' + self.filename)
            
        return loclist.__iter__()

    def location_has_file(self,fullpath):
        # optimize location checks for pnfs if mounted
        if fullpath[:6] == '/pnfs/' and self.have_pnfs and os.access(fullpath, os.R_OK):
            return 1
        res = self.ifdh_handle.ls(fullpath,1,'')
        return len(res) != 0

    def uncache_location(self,fp):
        pass

    def get_paths_for(self,filename):
        loclist = self.ifdh_handle.locateFile( filename )
        loclist = [ sampath(loc) + '/' + filename  for loc in loclist]
        return loclist
         
    def remove_path_for(filename, fp):
        pass

    def startProject(self, projname, station, dataset, user, experiment):
        return "fake://fake"

    def findProject(self, projname, station):
        return "fake://fake"

    def establishProcess(self,  purl, a, vers, hostname, user, pkg, desc, lim , schema):
        self.count = 0
        return "1"

    def updateFileStatus(self, purl, consumer_id,f, status):
        return 1

    def getNextFile(self, purl, consumer_id):
        self.count = self.count + 1
        if self.count == 1:
            return self.filename
        else:
            return ""
      
    def endProject(self, purl):
        return 1

    def wrap_ls(self, path, n, force):
        res = self.ifdh_handle.ls(path,n,force)
        return res

class fake_metacat_ifdh_handle:
    """ existing code calls ds.ifdh_handle.locateFile to get file
        locations, so we need a class that emulates locateFile using
        our rucio_client ... """
    def __init__(self):
        self.replica_client = ReplicaClient()
        self.actual_ifdh = ifdh.ifdh()

    def locateFile(self, f):
        """ ifdh locateFile just returns a list of directories where
            the file is, so get the replica list from rucio, and 
            convert to a list of directories """
        didlist = []
        fscope, fname = f.split(":")
        didlist.append( {'scope': fscope, 'name': fname })
        loclist = list(self.replica_client.list_replicas(didlist))
        locdict = loclist[0]
        # get rucio locations
        res = []
        for loc in locdict["pfns"]:
            res.append(os.path.dirname(loc.replace("davs:","https:")))
        return res

    def ls(self, path, n, force):
        return self.actual_ifdh.ls(path, n, force)

class dataset_metacat_dd:
    def __init__( self, did = None, verbose = 0):

        self.replica_client = ReplicaClient()
        self.ddisp = DataDispatcherClient()
        self.mcc = MetaCatClient()
        self.ih = ifdh.ifdh()
        self.last_reauth = 0
        self.verbose = verbose
        self.did = did
        self.ifdh_handle = fake_metacat_ifdh_handle()
        self.have_pnfs = os.access("/pnfs/", os.R_OK)
        self.dims = "metacat datset %s" % did

    def get_token(self):
        os.environ["IFDH_TOKEN_ENABLE"] = "1"
        tokenf = self.ih.getToken()
        if self.verbose > 0:
            sys.stderr.write("got token: %s\n" % tokenf)
        with open(tokenf, "rb") as tf:
             return tf.read().strip()

    def reauth(self):
        if time.time() - self.last_reauth < 3600:
            return
        last_reauth = time.time
        self.tokenbits = self.get_token()
        self.ddisp.login_token(os.environ.get("USER"), self.tokenbits)
        self.mcc.login_token(os.environ.get("USER"), self.tokenbits)
        self.last_reauth = time.time()

    def next_file_loop_thread(self, projid,  touch):

        self.reauth()
        l = threading.local()
        l.wid = self.ddisp.new_worker_id()
        if self.verbose > 0:
            sys.stderr.write("starting thread: worker id {}\n".format(l.wid))
            sys.stderr.write("calling next_file({0}, {1})\n".format(projid, l.wid))
       
        try:
            l.info = self.ddisp.next_file(projid, l.wid)
            
            while( isinstance( l.info, dict) ):
                self.reauth()
                if self.verbose > 0:
                    sys.stderr.write(repr(l.info) + "\n")

                l.fid = "%s:%s"%(l.info["namespace"],l.info["name"])

                replicas = l.info.get("replicas",{})
                if touch and replicas:
                    for rse in replicas:
                        requests.get(
                            replicas[rse]["url"].replace("davs:","https:"), 
                            headers={
                               'Range': 'bytes=1-4',
                               'Accept': '*/*',
                               'Authorization': 'Bearer %s' % self.tokenbits
                             },
                             verify="/etc/grid-security/certificates",
                        )
                        if verbose > 1:
                             sys.stderr.write(f"Touched {replicas[rse]['url']}\n")

                self.ddisp.file_done(projid, l.fid)
                l.info = self.ddisp.next_file(projid, l.wid)
        except metacat.common.exceptions.WebAPIError:
            pass

    def ddisp_prestage_files(self, nparallel: int=1, touch=False)->None:
        self.reauth()
        if self.verbose > 0:
            sys.stderr.write("starting prestage of {0}:\n".format(self.did))
        self.flist = self.mcc.get_dataset_files(self.did)
        proj = self.ddisp.create_project(files=self.flist)
        projid = proj['project_id']
        threads = []
        for i in range(nparallel):
            th = threading.Thread(target=self.next_file_loop_thread, args=[projid, touch])
            th.start()
            threads.append(th)
        time.sleep(5)
        for th in threads:
            th.join()


    def fullpath_iterator(self, fulllocflag = True, tapeset = None):
        """ mimic the old SAM-based one, fullocflag is for now always on"""

        class _fp_iter:

            def __init__(self, loclist, tapeset = None):
                self.outer = loclist.__iter__()
                self.inner = self.outer.__next__()["pfns"].__iter__()

            def __iter__(self):
                return self

            def next(self):
                return self.__next__()
           
            def __next__(self):
                try:
                    loc = self.inner.__next__()
                except StopIteration:
                    self.inner = self.outer.__next__()["pfns"].__iter__()
                    loc = self.inner.__next__()
                if loc:
                    return loc.replace("davs:", "https:")
                else:
                    raise StopIteration

        self.flist = self.mcc.get_dataset_files(self.did)
        didlist = []
        for f in self.flist:
            didlist.append( {'scope': f["namespace"], 'name': f["name"] })
        loclist = self.replica_client.list_replicas(didlist)
        return _fp_iter(loclist, tapeset)
        

    def location_has_file(self,fullpath):
        # optimize location checks for pnfs if mounted
        if fullpath[:6] == '/pnfs/' and self.have_pnfs and os.access(fullpath, os.R_OK):
            return 1
        res = self.ifdh_handle.ls(fullpath,1,'')
        return len(res) != 0

    def get_flist(self):
        flist = self.mcc.get_dataset_files(self.did)
        self.flist = ["%s:%s" %(f["namespace"], f["name"]) for f in flist]
        return self.flist

    def normalize_list(self, full_list):
        return full_list

    def get_paths_for(self, f):
        didlist = []
        fscope, fname = f.split(":")
        didlist.append( {'scope': fscope, 'name': fname })
        loclists = self.replica_client.list_replicas(didlist)
        # get rucio locations
        res = []
        for ldict in loclist[0]:
            pfn = ldict["pfn"]
            res.append(pfn)
        return pfn

    def file_iterator(self):
        self.flist = self.get_flist()
        if self.flist:
            return self.flist.__iter__()
        else:
            return None


class dataset:
    def __init__( self, name = None, dims = None ):

        self.ifdh_handle = ifdh.ifdh()   
        self.name = None
        if name and dims or (not name and not dims):
             raise ParameterError("either name or dims, not both")
        elif name:
            self.name = name
            self.dims = "defname:%s" % name
        elif dims:
            self.dims = dims

        self.flush()

    def startProject(self,projname, station, dataset, user, experiment):
        return self.ifdh_handle.startProject(projname, station, dataset, user, experiment)

    def findProject(self,projname, station):
        return  d.ifdh_handle.findProject(projname, station)

    def establishProcess(self, purl, a, vers, hostname, user, pkg, desc, lim , schema):
        return self.ifdh_handle.establishProcess( purl, a, vers, hostname, user, pkg, desc, lim , schema)

    def getNextFile(self,purl, consumer_id):
        return self.ifdh_handle.getNextFile(purl, consumer_id)

    def updateFileStatus(self, purl, consumer_id,f, status):
        return self.ifdh_handle.updateFileStatus(purl, consumer_id,f, status)
        return 1
 
    def endProject(self, purl):
        return self.ifdh_handle.endProject(purl)

    def wrap_ls(self, path, n, force):
        for attempt in range(3):
            if attempt > 0:
                logging.warning("retrying ifdh ls ('%s')" % path)
            try:
                res = self.ifdh_handle.ls(path,n,force)
                return res
            except:
                logging.exception("exception in ifdh ls('%s'):"% path)
                pass
        logging.warning("no more ls retries.")
        return []

    def flush(self):
        self.flist = None
        self.dircache = {}
        self.locmap = None

    def get_base_dir(self, fullpath):
        nspath = fullpath.replace('/','')
        nslashes = len(fullpath) - len(nspath) 
        isurl = fullpath.find('://') > 0
        if isurl:
           nslashes = nslashes - 2
        # always just look 1 direcory deep, too slow otherwise.
        # formerly:
        # #if it is a really deep location, start 2 subdirs in
        # #otherwise 1 subdir in
        #if nslashes > 4:
        #   trim = 2
        #else:
        trim = 1
        base = fullpath
        for i in range(trim):
            base = base[:base.rfind('/')]
        return base
 
    def normalize_list(self, fl):
        res = []
        for f in fl:
            f = f.replace('/pnfs/fnal.gov/usr/','/pnfs/')
            if f.find('s3:/') == 0 and f.find('s3://') != 0:
                f = f.replace('s3:/','s3://')
            for pat in ('//', '/./'):
                pos = f.rfind(pat)
                while pos > 7:
                    f = f[:pos] + f[(pos+len(pat)-1):]
                    pos = f.rfind(pat)
            res.append(f)
        return res

    def location_has_file(self, fullpath):
        if self.cached_location_has_file(fullpath):
            return 1
        else:
            res = self.wrap_ls(fullpath,1,'')
            return len(res) != 0

    def uncache_location(self,fullpath):
        base = self.get_base_dir(fullpath)
        if base in self.dircache:
            if fullpath in  self.dircache[base]:
                self.dircache[base].remove(fullpath)
        self.remove_path_for(os.path.basename(fullpath),os.path.dirname(fullpath))
                 
    def cached_location_has_file(self, fullpath):
        base = self.get_base_dir(fullpath)
        if base not in self.dircache:
            fl = self.normalize_list(self.wrap_ls(base, 3, ''))
            #print "got file list: ", fl
            self.dircache[base] = set(fl)
        return fullpath in self.dircache[base]


    def get_flist( self ):
        if self.flist == None:
            self.flist =  self.ifdh_handle.translateConstraints(self.dims)
        return self.flist

    def file_iterator(self):
        flist = self.get_flist()
        return flist.__iter__()
        
    class _loc_iterator(object):
        def __init__(self, locmap, fulllocflag = False, tapeset = None):
            #print "in _loc_iterator.__init__, locmap is:", locmap
            self.loc_iter = [].__iter__()
            self.locmap = locmap
            self.fulllocflag = fulllocflag
            self.tapeset = tapeset
            self.key_iter = list(locmap.keys()).__iter__()
            try:
                self.next_key()
            except StopIteration:
                pass

        def next_key(self):
            #print "in _loc_iterator.next_key..."
            self.curfile = next(self.key_iter)
 
            #print "curfile is: ",  self.curfile
            self.loc_iter = self.locmap[self.curfile].__iter__()

        def __iter__(self):
            return self

        def next(self):
            return self.__next__()
 
        def __next__(self):
            #print "in _loc_iterator.next..."
            res = None
            while res == None:
                try:
                    res = next(self.loc_iter)
                except StopIteration:
                    #print "in _loc_iterator.next, trying next key.."
                    # if *this* rasies StopIter, we bail...
                    self.next_key()

            #pre = res
            if not self.fulllocflag:
                prefix, res = res.split(':',1)

            m = re.search('\(.*?@(.*?)\)', res)
            if m:
                if self.tapeset != None:
                    self.tapeset.add(m.group(1))
                res = re.sub('\(.*?\)$','',res)

            #print "converted\n\t%s\nto\n\t%s" % ( pre, res)

            return res + '/' + self.curfile

    def get_paths_for(self, filename):
        self.get_locmap(fulllocflag = True)
        locs = self.locmap.get(filename,[])
        locs = [sampath(x) for x in locs]
        locs = self.normalize_list(locs)
        locs = [(x + '/' + filename) for x in locs]
        return locs

    def remove_path_for(self, filename, path):
        locs = [x for x in self.locmap.get(filename)]
        if len(locs) >  0:
            for i in range(len(locs)):
                if locs[i][locs[i].find(':')+1:] == path:
                    del locs[i:i+1]
                    break
            self.locmap[filename] = locs

    def get_locmap(self, fulllocflag = False):
        if self.locmap != None:
            return
        #print "get_locmap: starting", time.ctime()
        flist = self.get_flist()
        self.locmap = {}
        while len(flist) > 0:
            first_k = flist[:500]
            flist = flist[500:]
            self.locmap.update(self.ifdh_handle.locateFiles(first_k))
        #print "get_locmap: finishing", time.ctime()

    def fullpath_iterator(self, fulllocflag = False, tapeset = None):
        self.get_locmap(fulllocflag)
        return self._loc_iterator(self.locmap, fulllocflag, tapeset = tapeset)

def samtapeloc(dir):
    if dir.find("(") > 0:
        return dir[dir.find("("):dir.find(")")+1]
    else:
        return None

def sampath(dir):
    if dir.find("(") > 0 and dir.find(")") > dir.find("("):
       dir = dir[:dir.find("(")] + dir[dir.find(")")+1:]

    if dir.find("s3://") == 0:
       return dir

    if dir.find("s3:/") == 0:
       return dir[0:4]+dir[3:]

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
           pp = pp.decode()
           pp = pp.strip('\n')
           prefix, rest = pp.split(":",1)
           #print "checking:", pp, "->",  prefix,  ":", rest
           if dir.startswith(rest):
                return "%s:" % prefix
    except:
        print("exception in samweb list-data-disks...")
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
       #print "saw grid or experiment..."
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

UUID_RE=r'[0-9a-f]{8}(?:-[0-9a-f]+)+-[0-9a-f]+(?=[-.])'

def has_uuid_prefix(s):
    return bool(re.search(UUID_RE,s))

def replace_uuids(s):
    return re.sub(UUID_RE,'',s)

def check_destination(samweb,dest):
    # /pnfs is officially okay
    if dest.find('/pnfs/') == 0:
        return 1
    samdest = samprefix(dest) + sampath(dest)
    # or any known data disk..
    #print "samdest is:" , samdest
    for d in samweb.listDataDisks():
        #pos = samdest.find(d['mount_point'])
        #print "d is: " , d
        #print "d.mount_point %s pos: %d" % (d['mount_point'], pos)
        if samdest.find(d['mount_point']) == 0 or dest.find(d['mount_point']) == 0:
            return 1
    # otherwise, nope.
    return 0

def zerodeep(f):
    return '/'

def onedeep(f):
    h = hashlib.md5(f).hexdigest()
    return "/%s/" % h[0]

def twodeep(f):
    h = hashlib.md5(f.encode("utf-8")).hexdigest()
    return "/%s/%s/" % (h[0], h[1])

def threedeep(f):
    h = hashlib.md5(f.encode("utf-8")).hexdigest()
    return "/%s/%s/%s/" % (h[0], h[1], h[2])

def fourdeep(f):
    h = hashlib.md5(f.encode("utf-8")).hexdigest()
    return "/%s/%s/%s/%s/" % (h[0], h[1], h[2], h[3])

def doublesha256(f):
    h = hashlib.sha256(f.encode("utf-8")).hexdigest()
    return "/%s/%s/" % (h[0:2], h[2:4])

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
    dest = dest 
    if dest.startswith("s3:/") and dest[4] != '/':
        dest="s3://"+dest[4:]
    for p in loclist:
        if p.find(dest) != -1:
            print("Notice: file %s already has a copy at %s, skipping" % ( f, p))
            res = True
            break
    # print "already_there:", f, loclist, dest
    return res

def copy_and_declare(d, cpargs, locargs, dest, subdirf, samweb, just_say, verbose, paranoid , intermed = False):
    res = 0

    if len(cpargs) == 0:
        # nothing to do!
        return res

    # trailing ; confuses ifdh cp
    if cpargs[-1] == ';':
        cpargs = cpargs[:-1]

    if just_say:
        print("I would 'ifdh cp %s'" % cpargs)
        for f in locargs:
            print("I would declare location for %s of %s" % (f, dest+subdirf(f)))
    else: 
        logging.debug("doing ifdh cp %s" % cpargs)
        if intermed:
            # make two commandlines, src to intermed, intermed to dest
            l1 = []
            l2 = []
            first = True
            for a in cpargs:
                if ( a == ';' ):
                    l1.append(a)
                    l2.append(a)
                    first = True;
                    continue
                b = os.path.basename(a)
                i_f = "%s/%s" % workdir, b
                if first:
                    l1.append(f)
                    l1.append(i_f)
                    first=False
                else:
                    l2.append(i_f)
                    l2.append(f)
               
            if len(l1) == 2 and l1[0].find("s3://") == 0:
                res = os.system("aws s3 cp %s %s" % (l1[0], l1[1]))
            else:
                res = d.ifdh_handle.cp(l1)
            res = d.ifdh_handle.cp(l2)
        else:
            print("ifdh cp args; %s" % cpargs)
            res = d.ifdh_handle.cp(cpargs)
        logging.debug("ifdh cp returns %d" % res)
        # XXX note this is arguably incorrect, we only declare locations if
        #     *all* the copies in the batch succeed; but if *some* of them
        #     do we don't...
        if res == 0:
            logging.debug("doing locargs: %s", repr(locargs))
            for f in locargs:
                try:
                    if paranoid:
                         cpdest = dest +subdirf(f) + f
                         if cpdest.find("s3:/") == 0 and cpdest.find("s3://") != 0:
                            cpdest="s3://"+cpdest[4:]

                         if len(d.wrap_ls(cpdest,1,'')) == 0:
                            print("unable to verify copy to ", cpdest, " not declaring.")
                            continue

                    loc =  samprefix(dest) + dest + subdirf(f)
                    logging.debug("addFileLocation(%s, %s)" % (f, loc))
                    samweb.addFileLocation(f, loc )
                except:
                    raise
                    res = 1
    return res

def get_enstore_info(bfid , maxdepth = 3):
    if maxdepth <= 0:
        return None
    enstore_cmd = "enstore info --bfid=%s" % bfid
    pfd = os.popen(enstore_cmd,"r")
    ei = pfd.read()
    pfd.close()

    logging.debug('enstore info says: "%s"' % ei )
    enstore_info = ast.literal_eval(ei)
    if isinstance(enstore_info, list):
        enstore_info = enstore_info[0]
    package_id = enstore_info.get('package_id')
    if package_id and package_id != bfid:
         enstore_info = get_enstore_info(package_id, maxdepth - 1)
    return enstore_info

def update_dcache_tape_location(sp, f, samweb):
    logging.debug('checking: %s/.(use)(4)(%s)' % (sp,f))
    fd = open( "%s/.(use)(4)(%s)" % (sp, f), "r")
    l4 = fd.read().strip()
    fd.close()
    l4s = l4.split("\n")
    bfid = l4s[8]
    label = l4s[0]
    cookie = l4s[1]
    checksum = l4s[10]
    if label and ':' in label:
        label = None
        cookie = None
        enstore_info = get_enstore_info(bfid)
        label = enstore_info.get('tape_label', None)
        cookie = enstore_info.get('location_cookie',None)
        
    if label:
        if cookie and cookie[0] in "0123456789_":
            sequence = int(str(cookie).replace('_',''))
        else:
            sequence = 0

        fulloc = "%s%s(%s@%s)" % (samprefix(sp),sp, sequence,label)
        logging.debug('Adding tape label location for %s: %s' % (f, fulloc))
        samweb.addFileLocation( f, fulloc )

def validate( ds, just_say = False, prune = False, verbose = False, experiment = None , locality = False, list_tapes=False, tapeloc= False, location = []):
    samweb = SAMWebClient(experiment=experiment)
    res=0

    wait_for_dcache()

    if isinstance(locality, dict):
        counts = locality 
        counts['ONLINE'] = 0
    else:
        counts = {}

    if location:
        locationre = re.compile("|".join(location))
    else:
        locationre = re.compile(".")

    if isinstance(list_tapes, set):
        tapeset = list_tapes
    elif list_tapes:
        tapeset = set()
    else:
        tapeset = None

    for p in ds.fullpath_iterator(fulllocflag = True, tapeset = tapeset):
        tl = samtapeloc(p)
        sp = os.path.dirname(sampath(p))
        f = os.path.basename(p) 
        fp = "%s/%s" % (sp,f)
        samloc = dirname(p)

        logging.debug("p : %s sp: %s fp: %s " % (p, sp, fp))

        if not locationre.match(fp):
            logging.debug("skipping: %s" % fp)
            continue

        if just_say and not prune:
            print("I would: ifdh ls %s/%s 0" % (sp, f))
        else:
            if not ds.location_has_file(fp):
                print("missing: %s" % p)
                res = 1
                if prune:
                    if just_say:
                        print("I would remove location: %s for %s " % (samloc, f))
                    else:
                        print("removing location: %s for %s " % (samloc, f))
                        try:
                            samweb.removeFileLocation(f, samloc)
                            ds.uncache_location(fp)
                            print("-- location %s removed for %s" %(samloc,f))
                        except:
                            logging.error("Error: Removing file location: %s %s " % (f, samloc))
            else:
                if verbose: print("located: %s" % p)

            if locality  or tapeloc:
               
                if not p.startswith("enstore:/pnfs") and not p.startswith("dcache:/pnfs") and not fp.startswith("/pnfs"):
                     continue

                try:
                
                    if locality:
                        fd = open( "%s/.(get)(%s)(locality)" % (sp, f), "r")
                        loc = fd.read().strip()
                        fd.close()
                        if verbose: print("locality: %s\t%s" % (loc, f))
                        counts[loc] = counts.get(loc,0) + 1

                        stat = os.stat("%s/%s" % (sp, f))
                        counts["%s_size"%loc] = counts.get("%s_size"%loc,0) + stat.st_size

                    if tapeloc and tl == None:
                        update_dcache_tape_location(sp, f, samweb)

                except Exception as e:
                    logging.error("Exception checking PNFS info: %s" % e)
                    logging.error(traceback.format_exc())
                    logging.error("continuing...")
       

    for f in ds.file_iterator():
        l = ds.ifdh_handle.locateFile(f)
        print("file %s has %d locations" % (f, len(l)))
        if len(l) == 0:
           print("file %s has 0 locations" % f)
           res = 1

    return res


def clone( d, dest, subdirf = twodeep, just_say=False, batch_size = 1, verbose = False, experiment = None, ncopies=1, just_start_project = False, connect_project = False , projname = None, paranoid = False, intermed = False, getawscreds = False):
  
    if getawscreds:
        do_getawscreds()
    # avoid dest//file syndrome...
    if dest[-1] == '/':
       dest = dest[:-1]

    if dest.find('/pnfs') >= 0:
        wait_for_dcache()

    # make gridftp tool add directories
    os.environ['IFDH_GRIDFTP_EXTRA'] = '-cd -sync'
    cpargs = []
    locargs = []
    count = 0
    samweb = SAMWebClient(experiment = experiment)

    if not check_destination(samweb,dest):
        print("Destination: %s is not known to SAM" % dest);
        print("...maybe you wanted ifdh_fetch?")
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
        purl = d.findProject(projname, os.environ.get('SAM_STATION',experiment))
    else:
        try:
            purl = d.startProject(projname, os.environ.get('SAM_STATION',experiment), d.name, user, experiment)
        except:
            logging.exception()

        if not purl:
            logging.error("startProject failed.")
            sys.exit(1)
        time.sleep(6)

    if verbose or just_start_project:
        logging.info("%s %s %s %s %s", ("found" if connect_project else "started"), "project:", projname, "->", purl)

    if (just_start_project):
        return 

    kidlist = []
    for i in range(0,int(ncopies) - 1):
        res = os.fork()
        if res > 0:
           kidlist.append(res)
           logging.debug("started child %s", res)
        if res == 0:
           # we are a child
           kidlist = []
           break
        if res < 0:
           print("Could not fork!")

    # rebuild our SAMWebClient and ifdh handle if we forked...
    # we've seen confusion in some cases, so just to be safe...
    if int(ncopies) > 0:
        samweb = SAMWebClient(experiment = experiment)
        d.ifdh_handle = ifdh.ifdh()
       
    consumer_id = d.establishProcess( purl, "sam_clone_dataset", "1", hostname, user, "fife_utils", "sam_clone_dataset project", 0 , "")
    consumer_id = consumer_id.strip()
    logging.info("got consumer id: %s", consumer_id)

    if consumer_id == "":
         print("Error: could not establish sam consumer id for project: ", projname)
         sys.exit(1)

    furi = d.getNextFile(purl, consumer_id)

    # deal with single/double s3 urls silliness..

    cpdest = dest
    if dest.find("s3:/") == 0 and dest.find("s3://") != 0:
        cpdest="s3://"+dest[4:]

    while furi:

        logging.debug("got file uri: %s", furi)

        f = basename(furi)

        loclist = d.get_paths_for(f)

        if already_there(f, loclist, dest):
            d.updateFileStatus(purl, consumer_id,f, 'transferred')
            d.updateFileStatus(purl, consumer_id,f, 'consumed')
            furi = d.ifdh_handle.getNextFile(purl, consumer_id)
            continue
                
        if len(loclist) > 0:
            locargs.append(f)
            cpargs.append(loclist[0])
            cpargs.append(cpdest + subdirf(f) + f)
            cpargs.append(';')
            dodir(d.ifdh_handle, dest+subdirf(f))
        else:
            print("Notice: skipping file %s, no locations" % f)

        count = count + 1
        if count % batch_size == 0:

            # above loop leaves a trailng ';', which confuses things.
            copy_and_declare(d, cpargs, locargs, dest, subdirf, samweb, just_say, verbose, paranoid, intermed)
            cpargs = []
            locargs = []

        if getawscreds and  count % 10000 == 0:
            do_getawscreds()

        d.updateFileStatus(purl, consumer_id,f, 'transferred')
        d.updateFileStatus(purl, consumer_id,f, 'consumed')
        furi = d.getNextFile(purl, consumer_id)

    copy_and_declare(d, cpargs, locargs, dest, subdirf, samweb, just_say, verbose, paranoid, intermed)
    d.ifdh_handle.setStatus( purl, consumer_id, "completed")

    if kidlist:
       # we're the parent, and there are kids
       for i in range(0,len(kidlist)):
           try:
               os.waitpid(kidlist[i],0)
           except:
               pass

    if kidlist or int(ncopies) == 1:
       d.endProject(purl)

def clean_one(d, path, full, keep, exp):
    samweb = SAMWebClient(experiment = exp)
    filename = basename(full)
    loc = dirname(full)
    res = 5
    try:
        if keep:
            res = 0
        else:
            res = d.ifdh_handle.rm(path, '')
            if res != 0:
                logging.error("Error: rm %s failed" % path)
    except:
        err = d.ifdh_handle.getErrorText()
        if err.find("No such file") >= 0 or err.find("No match") >= 0:
            print("rm failed due to missing file, ignoring...")
            res = 0
        else:
            logging.error("Error: rm %s failed" % path)
            res = 2

    if res == 0:
        try:
            d.uncache_location(full)
            samweb.removeFileLocation(filename, loc)
            res = 0
        except: 
            logging.error("Error: removeFileLocation %s %s failed" % (filename, loc))
            res = 3

    sys.stdout.flush()
    sys.stderr.flush()
    return res

def unclone( d, just_say = False, delete_match = '.*', verbose = False, experiment = '', nparallel = 1 , keep = False):
    proccount = 0
    fail = 0
    succeed = 0
    samweb = SAMWebClient(experiment = experiment)
    for full in d.fullpath_iterator(True):
        logging.debug("looking at full path: %s", full)
        spath = sampath(full)
        logging.debug("looking at sampath: %s", spath)
        filename = basename(full)
        if just_say:
            if re.match(delete_match, full) or re.match(delete_match, spath):
                print("I would 'ifdh rm %s'" % sampath(full))
                print("I would remove location %s for %s" % (dirname(full), filename ))
        else:
            if re.match(delete_match, full) or re.match(delete_match, spath):
                logging.debug("matches: %s" , delete_match)
                pl = d.get_paths_for(filename)
                logging.debug("paths: %s" , pl)
                if len(pl) == 1:
                    print("NOT removing %s, it is the only location!" % full)
                    continue
                logging.info("removing: %s" , full)
                if full.find("s3:/") == 0:
                   path = full[0:4]+full[3:]
                else:
                   path = sampath(full)

                # clean path out of our cache, so we don't count it
                # next time.
                d.remove_path_for(filename, os.path.dirname(full))

                # unlink in background, wait if we
                # have nparallel ones running.

                pid = os.fork()
                if 0 == pid:
                    # child
                    os._exit(clean_one(d,path,full,keep,experiment))
                elif -1 == pid:
                    # fork failed...
                    print("Cannot fork!")
                    clean_one(d, path,full,keep,experiment)
                else: 
                    # parent            
                    proccount = proccount + 1
                    #print "started child ", pid, "proccount",  proccount
                    while proccount >= nparallel:
                       (wpid, wstat) = os.wait()
                       #print "child ", wpid, " completed status ", wstat
                       if os.WIFEXITED(wstat) and os.WEXITSTATUS(wstat) != 0 or os.WIFSIGNALED(wstat):
                           fail = fail + 1
                       else:
                           succeed = succeed + 1
                       proccount = proccount - 1
    else:
        pass
        #print "not matches: " , delete_match

    # clean up rm threads
    while proccount > 0:
       (wpid, wstat) = os.wait()
       if os.WIFEXITED(wstat) and os.WEXITSTATUS(wstat) != 0 or os.WIFSIGNALED(wstat):

           fail = fail + 1
       else:
           succeed = succeed + 1
       #print "child ", wpid, " completed status ", wstat
       proccount = proccount - 1

    if fail > 0:
       print("Error: %d unlink/undeclares failed" % fail)

    if succeed == 0:
       print("Notice: no matching files found")
      
    return fail

if __name__ == '__main__':
    os.environ['EXPERIMENT'] = 'nova'
    #d1 = dataset('mwm_test_6')
    d1 = dataset('rock_onlyMC_FA141003xa_raw3')
    count1=0
    for f in d1.file_iterator():
        print("file: ", f)
        count1=count1+1
    count2=0
    print("-------------")
    for l in d1.fullpath_iterator():
        print("loc:" ,  l)
        count2 = count2+1
    print("count1 " , count1 , " count2 " , count2)
    for exp in [ 'uboone', 'nova', 'minerva', 'hypot' ]:
        os.environ['EXPERIMENT'] = exp
        for d  in [ 's3:/nova-analysis/data/output/stuff', '/pnfs/%s/raw/' % exp, '/pnfs/%s/scratch' % exp , '/%s/data/' % exp, 'srm://smuosge.smu.edu/foo/bar?SFN=/data/%s/file' % exp, 'gsiftp://random.host/stuff/%s/file' % exp ]:
           print(d , "->", sampath(d), '//', samprefix(d))
