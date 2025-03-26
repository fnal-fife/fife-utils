#!/usr/bin/env python3

import os
import sys
import grp
import logging
import functools
import ifdh
import re
import json
from metacat.webapi.webapi import MetaCatClient
from metacat.webapi.webapi import AlreadyExistsError, BadRequestError


import time
import metadata_converter
from rucio.client.replicaclient import ReplicaClient
from rucio.client.rseclient import RSEClient
import samweb_client
from typing import List, Dict, Any


class Migrator:
    """class to do bulk migrations from SAM to Metacat"""

    def __init__(self, exp):
        """init: note our experiment, get SAM, metacat, and two rucio
        client objects, attach a MetadataConverter, etc."""
        self.experiment = exp
        self.samweb = samweb_client.SAMWebClient()
        self.metacat = MetaCatClient()
        self.rucio_replica = ReplicaClient()
        self.rucio_rse = RSEClient()
        self.last_reauth = 0
        self.reauth_window = 3600
        self.mdconv = metadata_converter.MetadataConverter(exp)
        self.scratch_rse = None
        self.persist_rse = None
        self.default_rse = None
        self.last_flist_repr = "[]"
        self.last_metadata = {}
        self.ih = ifdh.ifdh()

    def reauth(self):
        """if it's been awhile, refresh auth tokens for metacat, SAM"""
        logging.info("Reauthenticating.")
        if time.time() - self.last_reauth < self.reauth_window:
            return
        tfn = self.ih.getToken()
        with open(tfn, "r") as tf:
            tok = tf.read().strip()
        self.metacat.login_token(os.environ.get("USER"), tok)
        self.last_reauth = time.time()

    def samgetmultiplemetadata(self, flist: List[str]) -> List[Dict[str, Any]]:
        """fetch SAM metadata for list of files, with locations
        mostly just calls samweb.
        Keep the last one in case we get asked again (i.e.
        for SAM->metacat followed by sam->rucio)"""
        if self.last_flist_repr == repr(flist):
            return self.last_metadata

        # convert for SAM, who doesn't put scopes on filenames
        flist = [x.split(":")[-1] for x in flist]
        logging.debug("filtered list:" + repr(flist))
        rlist = []
        while len(flist) > 0:
            first_k = flist[:500]
            flist = flist[500:]
            rlist.extend( self.samweb.getMultipleMetadata(first_k, locations=True))
        self.last_metadata = rlist
        self.last_flist_repr = repr(flist)
        return self.last_metadata

    def mdsam2meta(self, mdlist, namespace):
        """convert a whole list of metadata"""
        res = []
        for m in mdlist:
            res.append(self.mdconv.convert_all_sam_mc(m, namespace))
        return res

    def mdmeta2sam(self, mdlist):
        """convert a whole list of metadata"""
        res = []
        for m in mdlist:
            res.append(self.mdconv.convert_all_mc_sam(m))
        return res

    @functools.cache
    def sam_data_disks(self):
        """fetch SAM data disk list, to figure out sam prefixes"""
        self.samweb.listDataDisks()

    def samprefix(self, dir):
        """stolen from other fife_utils, mostly.
        Pick the sam prefix for a directory path by looking through
        the known data-disks for a match; give local hostname if no match"""

        for pp in self.sam_data_disks():
            prefix, rest = pp.split(":", 1)
            if dir.startswith(rest):
                return "%s:" % prefix

        if dir.startswith("/pnfs/uboone/scratch"):
            return "fnal-dcache:"

        elif dir.startswith("/pnfs/%s/scratch" % os.environ.get("EXPERIMENT")):
            return "dcache:"

        elif dir.startswith("/pnfs/%s/persistent" % os.environ.get("EXPERIMENT")):
            return "dcache:"

        elif dir.startswith("/pnfs"):
            return "enstore:"

        elif dir.startswith("/grid/") or dir.startswith(
            "/%s/" % os.environ.get("EXPERIMENT", None)
        ):
            if os.environ.get("EXPERIMENT") in [
                "minerva",
            ]:
                return os.environ.get("EXPERIMENT") + "_bluearc:"
            else:
                return os.environ.get("EXPERIMENT") + "data:"

        else:
            return socket.gethostname() + ":"

    @functools.cache
    def getrselist(self):
        """get rse list, note ones that look like scratch or persistent"""
        rsedicts = self.rucio_rse.list_rses()
        rses = [x["rse"] for x in rsedicts]
        for r in rses:
            if r.find("SCRATCH") >= 0:
                self.scratch_rse = r
            elif r.find("PERSIST") >= 0:
                self.persist_rse = r
            else:
                self.default_rse = r
        return rses

    def loc2rse(self, pfn, rses):
        # this needs to be smarter, later, but for now...
        logging.debug(f"loc2rse: pfn {pfn} rses {repr(rses)}")
        if len(rses) == 1:
            return rses[0]
        return rses[0]

    def sam2rucio(self, flist: List[str], dsdid: str):
        """migrate file location info from SAM to rucio for list of files
        put them in datset dsid on Rucio"""
        logging.info(f"Migrating {len(flist)} files from sam to Ruio dataset {namespace}")
        logging.info(f"{repr(flist)}")
        flist = flist.copy()  # we're going to prune it, don't change parents
        dsscope, dsname = dsdid.split(":")
        rses = self.getrselist()
        mdlocs = self.samgetmultiplemetadata(flist)
        rse_files = {}
        for md in mdlocs:
            pfn = md["locations"][0]["full_path"]
            pfn = pfn + "/" + md["file_name"]
            rse = self.loc2rse(pfn, rses)
            if pfn.find("dcache:/pnfs/") == 0:
                # rucio gives davs: locations...
                pfn = pfn.replace(
                    "dcache:/pnfs/", "davs://fndcadoor.fnal.gov:2880/pnfs/fnal.gov/usr/"
                )

            if not rse in rse_files:
                rse_files[rse] = []
            csa = ""
            for csi in md["checksum"]:
                if 0 == csi.find("adler32:"):
                    logging.debug("found adler checksum: ", csi)
                    csa = csi.split(":")[1].strip()

            if not csa:
                logging.error(
                    f"No adler32 checksum for {md['file_name']} in SAM, cannot define to rucio"
                )
            else:
                rse_files[rse].append(
                    {
                        "scope": dsscope,
                        "name": md["file_name"],
                        "bytes": md["file_size"],
                        "adler32": csa,
                        "pfn": pfn,
                    }
                )
        for rse in rse_files:
            logging.debug(f"rse_files[{rse}] == {repr(rse_files[rse])}")
            self.rucio_replica.add_replicas(rse, rse_files[rse])

        contents = ({"name": fname, "scope": dsscope} for fname in flist)

        self.rucio_data.add_files_to_dataset(
            {"scope": dsscope, "name": dsname, "dids": contents}
        )

    def rucio2sam(self, flist: List[str]):
        """migrate file locations from rucio to SAM"""
        didlist = []
        for f in flist:
            fscope, fname = f.split(":")
            didlist.append({"scope": fscope, "name": fname})
        loclist = []
        while len(didlist) > 0:
            first_k = didlist[:500]
            didlist = didlist[500:]
            loclist.extend( self.rucio_replica.list_replicas(fist_k))
        # get rucio locations
        for ldict in loclist:
            pfn = ldict["pfn"]
            loc = os.path.dirname(pfn)
            fn = os.path.basename(pfn)
            pf = self.samprefix(pfn)
            self.samweb.addFileLocation(fn, "%s:%s" % (pf, loc))

    def sam2metacat(self, flist: List[str], dsdid):
        """migrate metadata from sam to metacat for list of files"""
        flist = flist.copy()  # we're going to prune it, don't change parents
        logging.info(f"Migrating {len(flist)} files from SAM to metacat dataset {dsdid}")
        logging.debug(f"{repr(flist)}")
        flistck = flist.copy()  # we're going to prune it, don't change parents
        dsscope, dsname = dsdid.split(":")

        alrdlist = []
        while len(flistck) > 0:
            first_k = flistck[:500]
            flistck = flistck[500:]
            alrdlist.extend(self.metacat.get_files([{"did": f"{dsscope}:{f}"} for f in first_k]))

        for dct in alrdlist:
            logging.info(f"dropping file {dct['name']}, already in metacat")
            pos = flist.index(dct["name"])
            flist.pop(pos)

        if not flist:
            logging.info("all files already in metacat...")
            return

        if dsscope in ("mu2e","dune","icarus"):
            owner = "pro"
        else:
            owner = dsscope
       
        try:
            self.metacat.create_namespace(dsscope, owner_role=owner)
        except AlreadyExistsError:
            pass
        except BadRequestError:  # create_namespace returns this for exists
            pass

        try:
            self.metacat.create_dataset(dsdid)
        except AlreadyExistsError:
            pass

        mdlist = []
        while len(flist) > 0:
            first_k = flist[:500]
            flist = flist[500:]
            mdlist.extend( self.samgetmultiplemetadata(first_k))
        logging.debug("sam metadata: " + json.dumps(mdlist, indent=2))
        mdlist2 = self.mdsam2meta(mdlist, dsscope)
        logging.debug("converted: " + json.dumps(mdlist2, indent=2))
        while len(mdlist2) > 0:
            first_k = mdlist2[:500]
            mdlist2 = mdlist2[500:]
            self.metacat.declare_files(dsdid, first_k, dsscope)

    def metacat2sam(self, flist: List[str]):
        """migrate metadata from metacat to sam for list of files"""
        flist = list(flist)
        logging.info(f"Migrating {len(flist)} files from metacat to SAM")
        logging.info(f"{repr(flist)}")
        while len(flist) > 0:
            first_k = flist[:500]
            flist = flist[500:]

            mdlist = self.metacat.get_files(
                [{"did": f"{f['namespace']}:{f['name']}"} for f in first_k], with_metadata=True, with_provenance=True
            )
            logging.debug("metacat metadata: ", repr(mdlist))
            mdlist2 = self.mdmeta2sam(mdlist)
            logging.debug("converted: ", repr(mdlist2))
            # find bulk-declare call in Fermi-FTS and use here?
            for m in mdlist2:
                try:
                    self.samweb.declareFile(md=m)
                except samweb_client.exceptions.FileAlreadyExists:
                    pass

    def get_sam_owner(self, ds):
        text = self.samweb.describe_definition(ds)
        owner = self.experiment
        for line in text.split("\n"):
            pos = line.find("Username:")
            if pos >= 0:
                owner = line[pos + 10 :]
        return owner

    def migrate_datasets_sam_mc(self, dslist):
        for ds in dslist:
            self.reauth()
            namespace = self.get_sam_owner(ds)
            flist = self.samweb.list_definition_files(ds)
            self.sam2metacat(flist, "%s:%s" % (namespace, ds))

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

    def mu2e_migrate_sam_mc(self, query):
        """
            Convert only files which have 6-component-dot names, like
            tier.owner.desc.config.seq.format
            there are many 8-dot files, and few others, which should be ignored.
            Every file has a 5-component-dot default dataset, found w/o seq as
            owner:tier.owner.desc.config.format
        """
        flist1 = m.samweb.listFiles(query)

        # group new files by dataset
        dslists = {}
        for fn in flist1:
            dl = fn.split(".")
            if len(dl) != 6:
                # only want 6-component names
                continue
            ds = f"{dl[1]}:{dl[0]}.{dl[1]}.{dl[2]}.{dl[3]}.{dl[5]}"
            if not ds in dslists:
                dslists[ds] = []
            dslists[ds].append(fn)

        # use sam2metacat to do each dataset
        for ds in dslists:
            m.sam2metacat(dslists[ds], ds)


if __name__ == "__main__":
    import argparse

    # default experiment
    experiment = os.environ.get(
        "EXPERIMENT", os.environ.get("SAM_EXPERIMENT", grp.getgrgid(os.getgid())[0])
    )

    ap = argparse.ArgumentParser("migrator", description="metadata migration utility")
    ap.add_argument(
        "-e",
        "--experiment",
        default=experiment,
        help="use this SAM instance defaults to $SAM_EXPERIMENT if not set",
    )
    ap.add_argument("--verbose", action="store_true", default=False)
    ap.add_argument("--debug", action="store_true", default=False)
    ap.add_argument("--sam-to-metacat", action="store_true", default=False)
    ap.add_argument("--sam-to-rucio", action="store_true", default=False)
    ap.add_argument("--metacat-to-sam", action="store_true", default=False)
    ap.add_argument("--rucio-to-sam", action="store_true", default=False)
    ap.add_argument("--mu2e-sam-to-metacat",  default=False, action="store_true", help = "migrate with mu2e-style name-based-datasets")
    ap.add_argument(
        "--query", help="metadata query to find files to migrate", default=None
    )
    ap.add_argument("--file-list", help="list of files/dids to migrate", default=None)
    ap.add_argument(
        "--file-list-file", help="file with list of files/dids to migrate", default=None
    )
    ap.add_argument(
        "--dest-dataset",
        help="dataset to add files to in Rucio or Metacat; scope/namespace of dataset will be used for files",
        default=None,
    )

    avs = ap.parse_args()

    if avs.debug:
        logging.basicConfig(level=logging.DEBUG, stream=sys.stderr, encoding='utf-8')
        logging.getLogger("urllib3").setLevel(logging.DEBUG)
    elif avs.verbose:
        logging.basicConfig(level=logging.INFO, stream=sys.stderr, encoding='utf-8')

    # make sure we have the environment variables set to talk to everything
    bail_ev = False
    for ev in ['METACAT_SERVER_URL', 'RUCIO_HOME', 'SAM_EXPERIMENT' ]:
        if not os.environ.get(ev,False):
            logging.error(f"ERROR: {ev} not set in envrionment.")
            bail_ev = True
    if bail_ev:
        exit(1)

    m = Migrator(avs.experiment)
    m.reauth()

    if not (
        avs.sam_to_metacat or avs.sam_to_rucio or avs.metacat_to_sam or avs.rucio_to_sam or avs.mu2e_sam_to_metacat
    ):
        logging.error("Notice: no actions requested, all done!")
        ap.print_help()
        sys.exit(0)

    if avs.query and avs.file_list or avs.query and avs.file_list_file:
        logging.error("Error: need either a query or a file list, but not both")
        sys.exit(1)

    if not (avs.query or avs.file_list or avs.file_list_file):
        logging.error("Error: need (--query or --file-list or --file-list-file")
        sys.exit(1)

    if (avs.sam_to_metacat or avs.sam_to_rucio) and not avs.dest_dataset:
        logging.error("Error: need a --dest-dataset.")
        sys.exit(1)

    if (avs.sam_to_metacat or avs.sam_to_rucio) and avs.query:
        flist = m.samweb.listFiles(avs.query)

    if (avs.metacat_to_sam or avs.rucio_to_sam) and avs.query:
        flist = m.metacat.query(avs.query)

    if avs.file_list:
        flist = re.split(r"\s+", avs.file_list)

    if avs.file_list_file:
        with open(avs.file_list_file, "r") as f:
            data = f.read().strip()
        flist = re.split(r"\s+", data)

    try:

        if avs.mu2e_sam_to_metacat:
            m.mu2e_migrate_sam_mc(avs.query)

        if avs.sam_to_metacat:
            m.sam2metacat(flist, avs.dest_dataset)

        if avs.sam_to_rucio:
            m.sam2rucio(flist, avs.dest_dataset)

        if avs.metacat_to_sam:
            m.metacat2sam(flist)

        if avs.rucio_to_sam:
            m.rucio2sam(flist)

    except Exception as e:
        logging.exception("Exception during migration")
        raise

    logging.info("Done.")
