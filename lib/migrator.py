#!/usr/bin/env python3

import os
import sys
import grp
import functools
from metacat.webapi.webapi import MetaCatClient
import time
import metadata_converter
from rucio.client.replicaclient import ReplicaClient
from rucio.client.replicaclient import ReplicaClient
from  rucio.client.rseclient import RSEClient
import samweb_client
from typing import List, Dict, Any

class Migrator:
    """ class to do bulk migrations from SAM to Metacat """

    def __init__(self, exp):
        """ init: note our experiment, get SAM, metacat, and two rucio
            client objects, attach a MetadataConverter, etc."""
        self.experiment = exp
        self.samweb = samweb_client.SAMWebClient()
        self.metacat = MetaCatClient()
        self.rucio_replica = ReplicaClient()
        self.rucio_rse = RSEClient()
        self.last_reauth = 0
        self.mdconv = metadata_converter.MetadataConverter(exp)
        self.scratch_rse = None
        self.persist_rse = None
        self.default_rse = None
        self.last_flist_repr = "[]"
        self.last_metadata = {}

    def reauth(self):
        """ if it's been awhile, refresh auth tokens for metacat, SAM """
        if time.time() - self.last_reauth < self.reauth_window:
            return
        tfn = self.ih.getToken()
        with open(tfn,"r") as tf:
            tok = tf.read().strip()
        self.metacat.login_token(os.environ.get("USER"), tok)
        pf = self.ih.getProxy()
        self.last_reauth = time.time()
        

    def samgetmultiplemetadata(self, flist: List[str]) -> List[Dict[str,Any]]:
        """ fetch SAM metadata for list of files, with locations
            mostly just calls samweb.
            Keep the last one in case we get asked again (i.e. 
            for SAM->metacat followed by sam->rucio)"""
        if self.last_flist_repr == repr(flist):
            return self.last_metadata

        # convert for SAM, who doesn't put scopes on filenames
        flist = [x.split(":")[-1] for x in flist]
        print("filtered list:" , repr(flist))
        self.last_metadata = self.samweb.getMultipleMetadata(flist, locations=True)
        self.last_flist_repr = repr(flist)
        return self.last_metadata

    def mdsam2meta(self, mdlist, namespace):
        """ convert a whole list of metadata """
        res = []
        for m in mdlist:
            res.append(self.mdconv.convert_all_sam_mc(m, namespace))
        return res

    def mdmeta2sam(self, mdlist):
        """ convert a whole list of metadata """
        res = []
        for m in mdlist:
            res.append(self.mdconv.convert_all_mc_sam(m))
        return res

    @functools.cache
    def sam_data_disks(self):
        """ fetch SAM data disk list, to figure out sam prefixes """
        self.samweb.listDataDisks()

    def samprefix(self,dir):
        """ stolen from other fife_utils, mostly.
            Pick the sam prefix for a directory path by looking through
            the known data-disks for a match; give local hostname if no match """

        for pp in self.sam_data_disks():
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
        """ get rse list, note ones that look like scratch or persistent """
        rsedicts = self.rucio_rse.list_rses()
        rses = [ x["rse"] for x in rsedicts ]
        for r in rses:
            if r.find("SCRATCH") >= 0:
                self.scratch_rse = r
            elif r.find("PERSIST") >= 0:
                self.persist_rse = r
            else:
                self.default_rse = r
        return rses

    def sam2rucio(self, flist: List[str], dsdid: str):
        """ migrate file location info from SAM to rucio for list of files 
            put them in datset dsid on Rucio"""
        dsscope, dsname = dsdid.split(":")
        rses = self.getrselist()
        mdlocs =  self.samgetmultiplemetadata(flist)
        rse_files = {}
        for md in mdlist:
            pfn = md['locations'][0]['full_path']
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
        """ migrate file locations from rucio to SAM """
        didlist = []
        for f in flist:
            fscope, fname = f.split(":")
            didlist.append( {'scope': fscope, 'name': fname })
        loclist = self.rucio_replica.list_replicas(didlist)
        # get rucio locations
        for ldict in loclist:
            pfn = ldict["pfn"]
            loc = os.path.dirname(pfn)
            fn = os.path.basename(pfn)
            pf = self.samprefix(pfn)
            self.samweb.addFileLocation(fn, "%s:%s" % (pf, loc))

    def sam2metacat(self, flist: List[str], dsdid):
        """ migrate metadata from sam to metacat for list of files """
        dsscope, dsname = dsdid.split(":")
        mdlist =  self.samgetmultiplemetadata(flist)
        print("sam metadata: ", repr(mdlist))
        mdlist2 = self.mdsam2meta(mdlist, dsscope)
        print("converted: ", repr(mdlist2))
        self.metacat.declare_files(dsdid, mdlist2, dsscope)

    def metacat2sam(self, flist: List[str]):
        """ migrate metadata from metacat to sam for list of files """
        mdlist = self.metacat.get_files( [ {'did': f } for f in flist], with_metadata=True, with_provenance=True)
        mdlist2 = self.mdmeta2sam(mdlist)
        # find bulk-declare call in Fermi-FTS and use here?
        for m in mdlist2:
            self.samweb.declareFile(md=m)

    def get_sam_owner(self, ds):
        text = self.samweb.describe_definition(ds)
        owner = self.experiment
        for line in text.split("\n"):
            pos = line.find("Username:")
            if pos >= 0:
                owner = line[pos+10:]
        return owner

    def migrate_datasets_sam_mc(self, dslist):
        for ds in dslist:
            self.reauth()
            namespace = self.get_sam_owner(ds)
            flist = self.samweb.list_definition_files(ds)
            self.sam2metacat(flist, "%s:%s" %(namespace, ds))
            
    def migrate_datasets_mc_sam(self, dslist):
        for ds in dslist:
            self.reauth()
            flist = self.rucio_blah.list_files(ds)
            self.metacat2sam(flist)

    def migrate_mc_sam_since(self, date):
        self.reauth()
        flist = self.metacat.query(f"files where created_timestamp > '{date}'")
        self.metacat2sam(flist)
        self.rucio2sam(flist)

    def migrate_sam_mc_since(self, date):
        dsdid = f"{self.experiment}:new_files_since_{date}"
        flist = self.samweb.ListFiles("create_date > 'date'")
        self.sam2metacat(flist, dsdid)
        self.sam2rucio(flist, dsdid)

if __name__ == '__main__':
    import argparse

    # default experiment
    experiment = os.environ.get(
        "EXPERIMENT", os.environ.get("SAM_EXPERIMENT", grp.getgrgid(os.getgid())
[0])
    )

    ap = argparse.ArgumentParser("migrator", description="metadata migration utility")
    ap.add_argument(
        "-e", "--experiment",
        default=experiment,
        help="use this SAM instance defaults to $SAM_EXPERIMENT if not set",
    )
    ap.add_argument( "--sam-to-metacat", action="store_true", default=False)
    ap.add_argument( "--sam-to-rucio",   action="store_true", default=False)
    ap.add_argument( "--metacat-to-sam", action="store_true", default=False)
    ap.add_argument( "--rucio-to-sam",   action="store_true", default=False)
    ap.add_argument( "--query", help="metadata query to find files to migrate", default = None)
    ap.add_argument( "--file_list", help="list of files/dids to migrate", default = None)
    ap.add_argument( "--file_list_file", help="file with list of files/dids to migrate", default = None)
    ap.add_argument( "--dest-dataset", help="dataset to add files to in Rucio or Metacat; scope/namespace of dataset will be used for files", default=None)
 
    avs = ap.parse_args()

    m = Migrator(avs.experiment)

    if not (avs.sam_to_metacat or avs.sam_to_rucio or avs.metacat_to_sam or avs.rucio_to_sam ):
        print("Notice: no actions requested, all done!")
        ap.print_help()
        sys.exit(0)

    if avs.query and avs.file_list or avs.query and avs.file_list_file:
        print("Error: need either a query or a file list, but not both")
        sys.exit(1)

    if not (avs.query or avs.file_list or avs.file_list_file):
        print("Error: need (--query or --file-list or --file-list-file")
        sys.exit(1)

    if (avs.sam_to_metacat or avs.sam_to_rucio) and not avs.dest_dataset:
        print("Error: need a --dest-dataset." )
        sys.exit(1)

    if avs.sam_to_metacat or avs.sam_to_rucio and avs.query:
        flist = m.samweb.listFiles(avs.query)

    if avs.metacat_to_sam or avs.rucio_to_sam and avs.query:
        flist = m.metacat.query(avs.query)
       
    if avs.file_list:
        flist = re.split(r"\s+", avs.file_list)

    if avs.file_list_file:
        with open(avs.file_list_file, "r") as f:
            data = f.read()
        flist = re.split(r"\s+", data)


    if avs.sam_to_metacat:
        m.sam2metacat( flist, avs.dest_dataset)

    if avs.sam_to_rucio:
        m.sam2rucio( flist, avs.dest_dataset)

    if avs.metacat_to_sam:
        m.metacat2sam( flist )

    if avs.rucio_to_sam:
        m.rucio2sam( flist )

    print("Done.")
