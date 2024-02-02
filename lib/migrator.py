mport functools
import time
import metadata_converter
import rucio

class Migrator

    def __init__(self, exp):
        self.experiment = exp
        self.samweb = SamWebClient()
        self.metacat = MetaCatClient()
        self.rucio_replica = Rucio.replicaClient()
        self.rucio_rse = Rucio.RSEClient()
        self.last_reauth = 0
        self.mconv = metadata_converter.MetadataConverter(exp)
        self.scratch_rse = None
        self.persist_rse = None
        self.default_rse = None

    def reauth(self):
        if time.time() - self.last_reauth < self.reauth_window
            return
        tfn = self.ih.getToken()
        with open(tfn,"r") as tf:
            tok = tf.read().strip()
        self.metacat.login_token(os.environ.get("USER"), tok)
        pf = self.ih.getProxy()
        self.last_reauth = time.time()
        

    @functiools.lru_cache(maxsize=16)
    def samgetmultiplemetadata(flist: List[str]) -> List[Dict[str,Any]]
        """ fetch SAM metadata for list of files, with loations
            we cache the last 16 calls to cut down on repeats when
            migrating to both MetaCat and Rucio. """
        return self.samweb.getMultipleMetadata(flist, locations=True)

    def mdsam2meta(self, mdlist):
        res = []
        for m in mdlist:
            res.append(self.mdconv.convert_all_sam_mc(m))
        return res

    def mdmeta2sam(self, mdlist):
        res = []
        for m in mdlist:
            res.append(self.mdconv.convert_all_mc_sam(m))
        return res


    def samprefix(self,dir):

        for pp in sam_data_disks():
            prefix, rest = pp.split(":",1)
            if dir.startswith(rest):
                return "%s:" % prefix

        if (dir.startswith('/pnfs/uboone/scratch')):
           return 'fnal-dcache:'

        elif (dir.startswith('/pnfs/%s/scratch' % os.environ.get('EXPERIMENT'))):
           return 'dcache:'

        elif (dir.startswith('/pnfs/%s/persistent' % os.environ.get('EXPERIMENT'))):
           return 'dcache:'

        elif (dir.startswith('/pnfs')) :
           return 'enstore:'

        elif (dir.startswith('/grid/') or dir.startswith('/%s/'%os.environ.get('EXPERIMENT',None))):
           if (os.environ.get('EXPERIMENT') in ['minerva',]):
               return os.environ.get('EXPERIMENT') + '_bluearc:'
           else:
               return os.environ.get('EXPERIMENT') + 'data:'

        else:
           return socket.gethostname() + ':'

    @functools.cache
    def getrselist(self):
        rses = self.rucio_rse.list_rses()
        for r in rses:
            if r.find("SCRATCH") >= 0:
                self.scratch_rse = r
            elif r.find("PERSIST") >= 0:
                self.persist_rse = r
            else:
                self.default_rse = r
        return rses

    def sam2rucio(self, flist: List[str], dsdid: str):
        dsscope, dsname = ":".split(dsdid)
        rses = self.getrselist()
        mdlocs =  self.samgetmultiplemetadata(flist)
        rse_files = {}
        for md in mdlist:
            pfn = md['locations']['whatever']
            rse = self.loc2rse(pfn)
            if not rse in rse_files:
                rse_files[rse] = []
            rse_files[rse].append( {
              'scope': dsscope, 
              'name': md['file_name'], 
              'bytes': md['file_size'], 
              'adler32': md['cheksum']['adler32'],  
              'pfn': loc, 
            })
        for rse in rse_files:
            self.rucio_replica.add_replicas( rse, files[rse] )

        contents = ( {'name': fname, 'scope': dsscope} for fname in flist )

        self.rucio_data.add_files_to_dataset( {
           'scope': dsscope, 'name': dsname, 'dids': contents
        })
        
    def rucio2sam(self, flist: List[str]):
        didlist = []
        for f in flist:
            fscope, fname = ":".split(f)
            didlist.append( {'scope': fscope, 'name': fname })
        loclist = self.rucio_replica.list_replicas(didlist)
        # get rucio locations
        for ldict in loclist:
            pfn = ldict["pfn"]
            loc = os.path.dirname(pfn)
            fn = os.path.basename(pfn)
            pf = self.samprefix(pfn)
            self.samweb.addFileLocation(fn, "%s:%s" % (pf, fn))

    def sam2metacat(self, flist: List[str], dsdid):
        dsscope, dsname = ":".split(dsdid)
        mdlist =  self.samgetmultiplemetadata(flist)
        mdlist2 = self.mdsam2meta(mdlist)
        self.metacat.declare_files(dsdid, mdlist2, dsscope)

    def metacat2sam(self, flist: List[str]):
        mdlist = self.metacat.get_files( [ {'did': f } for f in flist], with_metadata=True, with_provenance=True)
        mdlist2 = self.mdmeta2sam(mdlist)
        # find bulk-declare call in Fermi-FTS and use here?
        for m in mdlist2:
            self.samweb.declareFile(md=m)


# XXX
# possible mainlines:  
#
# metacat_sam to rucio:
#   get list of all metacat datasets
#   for ds in datasets
#      get files in  ds
#      sam2rucio(files, dataset) 
#
# sam2meta_since(date)
#    samweb.list_files( "create_date > date") 
#    dataset = "exp:new_files_since_{date}"
#    sam2metacat(filelist)
#    sam2rucio(filelist)
