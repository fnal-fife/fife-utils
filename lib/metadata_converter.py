""" routines to convert file metadata between SAM and MetaCat """
import functools
import datetime

def convert_noop(x):
    """ identity function -- no conversion """
    return x

def convert_date_mc_sam(date):
    """ date format convert metacat to sam """
    dt = datetime.datetime.fromtimestamp(date)
    return dt.isoformat()

def convert_date_sam_mc(date):
    """ date format convert sam to metacat """
    dt = datetime.datetime.fromisoformat(date)
    return dt.timestamp()

def convert_checksums_sam_mc(checksums):
    """ special case: checksum conversion"""
    res = {}
    for cs in checksums:
        ctype, value = cs.split(":")
        res[ctype] = value
    return res

def convert_checksums_mc_sam(checksums):
    res = []
    for ctype in checksums:
        res.append( "%s:%s" % (ctype, checksums[ctype]))
    return res

def convert_runs_sam_mc(runs):
    """ special case: run/subrun  number conversion"""
    res = []
    m = self.run_scale_factor[self.exp]
    for r, sr, typ in runs:
        res.append(m * r + sr)
    return res


def convert_runs_mc_sam( runs):
    """ special case: run/subrun  number conversion"""
    res = []
    m = self.run_scale_factor[self.exp]
    for r in runs:
        rn = r / m
        sr = r % m
        typ = "mc" # so far, they're all montecarlo
        res.append( [rn, sr, typ] )
    return res

def convert_parents_sam_mc( parents):
    """ special case: parentage conversion"""
    res = []
    for i in parents:
        res.append( {
            "fid":  str(i["file_id"]),
            "name": i["file_name"],
            "namespace": self.experiment,  # XXX possible bug, what namespace?
        })
    return res

def convert_parents_mc_sam( parents):
    """ special case: parentage conversion"""
    res = []
    for i in parents:
        res.append( {
           "file_id": i["file_id"],
           "file_name": i["name"]
        })
    return res


class MetadataConverter:
    """ converts metadata between MetaCat and Sam"""
    def __init__(self, experiment):
        self.experiment = experiment
        #
        # conversion table:
        #
        self.conversion_mc_sam = {
            "mu2e": {
                "name": "file_name",
                "fid": "file_id",
                "size": "file_size",
                "checksums": "checksum",
                "parents": "parents",
                "creator": "user",
                "created_timestamp": "create_date",
                "updated_by": "update_user",
                "updated_timestamp": "update_date",

                "metadata:dh.type":  "file_type",
                "metadata:dh.status":  "content_status",

                "metadata:dh.dataset": "dh.dataset",
                "metadata:fn.tier": "data_tier",
                "metadata:fn.owner": "dh.owner",
                "metadata:fn.description": "dh.description",
                "metadata:fn.configuration": "dh.configuration",
                "metadata:fn.sequencer": "dh.sequencer",
                "metadata:fn.format": "file_format",

                "metadata:rs.first_run": "dh.first_run_event",
                "metadata:rs.last_run": "dh.last_run_event",
                "metadata:rs.first_subrun": "dh.first_subrun",
                "metadata:rs.last_subrun": "dh.last_subrun",
                "metadata:rs.runs": "Runs",
                "metadata:rse.first_run": "dh.first_run_event",
                "metadata:rse.last_run": "dh.last_run_event",
                "metadata:rse.first_subrun": "dh.first_subrun_event",
                "metadata:rse.last_subrun": "dh.last_subrun_event",
                "metadata:rse.first_event": "dh.first_event",
                "metadata:rse.last_event": "dh.last_event",
                "metadata:rse.nevent": "event_count",

                "metadata:gen.count": "dh.gencount",
                "metadata:app.family": "family",
                "metadata:app.name": "name",
                "metadata:app.version": "version",
            },
            "hypot": {
                "name": "file_name",
                "fid": "file_id",
                "size": "file_size",
                "checksums": "checksum",
                "parents": "parents",
                "creator": "user",
                "created_timestamp": "create_date",
                "updated_by": "update_user",
                "updated_timestamp": "update_date",

                "metadata:file.type":  "file_type",
                "metadata:file.status":  "content_status",
                "metadata:file.nevent": "event_count",

                "metadata:app.family": "family",
                "metadata:app.name":   "name",
                "metadata:app.version": "version",
            },
        }
        self.build_inverse()

        # constants for run/subrun combination
        self.run_scale_factors = { 
            "mu2e": 1000000, 
            "dune": 1000000, 
            "hypot": 1000,
        }

    def build_inverse(self):
        """ build the inverse mapping from the forward one"""
        self.conversion_sam_mc = {}
        for k in self.conversion_mc_sam:
            self.conversion_sam_mc[k] = {}
            for k2 in self.conversion_mc_sam[k]:
                v = self.conversion_mc_sam[k][k2]
                self.conversion_sam_mc[k][v] = k2



    def convert_all_sam_mc(self,md, namespace):
        res = {"namespace": namespace, "metadata":{}}
        for k, v in md.items():
            if not k in self.conversion_sam_mc[self.experiment]:
                continue
            # most things need no conversion, except...
            converter = convert_noop
            if k == "file_id":
                converter = str 
            if k == "checksum":
                converter = convert_checksums_sam_mc
            if k == "parents":
                converter = convert_parents_sam_mc
            if k == "runs":
                converter = convert_runs_sam_mc
            if k in [ "create_date", "update_date"]:
                converter = convert_date_sam_mc
            # in metacat, some things are in the metadata sub-dictionary
            # in this case the conversion key starts with metadata:
            ck = self.conversion_sam_mc[self.experiment][k]
            if ck.startswith("metadata:"):
                ck = ck.replace("metadata:","")
                res["metadata"][ck] = converter(v)
            else:
                res[ck] =  converter(v)

        return res

    def convert_all_mc_sam(self, md):
        for k, v in md.items:
            if not k in conversion_mc_sam[self.experiment]:
                continue
            ck = conversion_mc_sam[self.experiment][k]
            # most keys don't require conversion, except...
            converter = convert_noop
            if k == "fid":
               # cannot generally convert metacat fids to SAM integer file ids..
               continue
            if k == "parents":
                converter = convert_parents_mc_sam
            if k == "checksum":
                converter = convert_checksums_mc_sam
            if k == "runs":
                converter = convert_runs_mc_sam
            if k in [ "created_timestamp", "updated_timestamp" ]:
                converter = convert_date_mc_sam
            res[ck] = converter(v)
        # some things are in the metadata sub-dictionary...
        # they are listed as metadata:x in the table
        for k, v in md["metadata"].items():
            converter = convert_noop
            if not ("metadata:"+k) in conversion_mc_sam[self.experiment]:
                continue
            ck = conversion_mc_sam[self.experiment]["metadata:"+k]
            res[ck] = converter(v)

