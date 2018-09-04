
"""

Path template format


The template contains palce holders specified by ${...}

The content can be a simple field from the metadata, a category.param name from the metadata
or the special values run_number, run_type, app_name, app_family, app_version, year, month, day

Numeric values may be further qualified by the operators % (modulus) or / (division)
Finally there can be a length field in square brackets at the end. If the value is prefixed by '='
then it is treated as an exact length , otherwise it is a minimum. If the length is followed by '/' and a value
then the value is split into chunks of that size, separated by / characters

Examples:
If the run number is 123456

${run_number} gives 123456
${run_number[8]} gives 00123456
${run_number/100[6]} gives 001234
${run_number[2]} gives 123456
${run_number[=2]} gives 56
${run_number[8/2]} gives 00/12/34/56

"""
from __future__ import print_function

from builtins import str
from past.builtins import basestring
from builtins import object
import string, re, os.path
from datetime import datetime
import collections

class CaseInsensitiveDict(collections.MutableMapping):
    """ A case insensitve dictionary (for metadata, etc) """
    __slots__ = ['_data']

    def __init__(self, data=None, **kwargs):
        self._data = {}
        if data is None: data = {}
        self.update(data, **kwargs)

    def _get_lkey(self, key):
        if isinstance(key, basestring): return key.lower()
        else: return key

    def __getitem__(self, key):
        return self._data[self._get_lkey(key)][1]

    def __setitem__(self, key, value):
        # store the original key as well as the value
        self._data[self._get_lkey(key)] = (key, value)

    def __delitem__(self, key):
        del self._data[self._get_lkey(key)]

    def __iter__(self):
        # Return the original, cased, keys
        return (key for key, _ in self._data.values() )

    def __len__(self):
        return len(self._data)

    def copy(self):
        n = CaseInsensitiveDict()
        n._data = self._data.copy()
        return n
    def __repr__(self):
        return '%s(%r)' % (self.__class__.__name__, dict(list(self.items())))

    def __eq__(self, other):
        if isinstance(other, CaseInsensitiveDict):
            other_data = dict( (lk, v) for lk, (k,v) in other._data.items() )
        if isinstance(other, collections.Mapping):
            other_data = dict( (self._get_lkey(k),v) for k,v in other.items() )
        else:
            return NotImplemented
        # compare lower cased keys
        return dict( (lk, v) for (lk, (k,v)) in self._data.items() ) == other_data 

class _MDTemplate(string.Template):
    idpattern = r'[_a-z][_a-z0-9]*(?:\.[_a-z0-9]+)?(?:[%/][0-9]+)?(?:\[=?[0-9]+(?:/[0-9]+)?\])?'

def _convert_timeval(v):
    # convert from string iso format
    if isinstance(v, basestring):
        return datetime.strptime(v, '%Y-%m-%dT%H:%M:%S')
    else:
        # assume int/float and just return it
        return datetime.utcfromtimestamp(v)

class _MDMapping(object):
    def __init__(self, metadata, mtime, srcdir, basedir):
        self.md = metadata.copy()
        self.srcdir = srcdir
        self.basedir = basedir if basedir else self.srcdir
        # Deal with situations where only one of startTime and endTime is set, or where
        # the start is earlier than the end
        mdtime = None
        if metadata and metadata.get('start_time') is not None:
            mdtime = _convert_timeval(metadata['start_time'])
        if metadata and metadata.get('end_time') is not None:
            endtime = _convert_timeval(metadata['end_time'])
            mdtime = max(mdtime, endtime) if mdtime else endtime
        if mdtime is not None:
            self.date = mdtime
        else:
            self.date = datetime.utcfromtimestamp(mtime)
        
    def __getitem__(self, key):
        key = key.lower()
        pos = key.find('[')
        length = None
        fixedlength = False
        sublength = None

        #check for [...] at end
        if pos > 0:
            pos2 = key.find(']',pos)
            lengthpart = key[pos+1:pos2]
            key = key[:pos]
            # check for exact length specifier
            if lengthpart[:1] == '=':
                fixedlength = True
                lengthpart = lengthpart[1:]
            elif '/' in lengthpart:
                # check for slash
                pos = lengthpart.find('/')
                sublength = int(lengthpart[pos+1:])
                lengthpart = lengthpart[:pos]
            length = int(lengthpart)

        m = re.match(r'([^/%]+)([%/])(.*)', key)
        try:
            if m:
                denom = int(m.group(3))
                val = self._getValue(m.group(1))
                try:
                    val = int(val)
                except ValueError:
                    pass
                else:
                    if m.group(2) == '%':
                        val = val % denom
                    elif m.group(2) == '/':
                        val = val // denom # ensure integer division
            else:
                val = self._getValue(key)

            if length:
                # formats are only supported for integer values
                try:
                    val = int(val)
                except ValueError: pass
                else:
                    val = '%0*d' % (length, val)
                    if fixedlength: val = val[-length:]
                    if sublength:
                        import itertools
                        # Split the results into chunks of size sublength. The use of reversed is so any padding is applied at the beginning, not the end
                        val = '/'.join( reversed([''.join(reversed(i)) for i in itertools.zip_longest(fillvalue='0', *([iter(reversed(val))] * sublength))]))

            return val 
        except KeyError:
            return "None"

    def _getValue(self, key):
        if key == 'srcpath':
            return self.srcdir
        elif key == 'basepath':
            return self.basedir
        elif key == 'relpath':
            if not self.basedir or not self.srcdir: return ''
            return os.path.relpath(self.srcdir, self.basedir)
        if key == 'run_number':
            # there may be a list of run numbers - return the first one
            runs = self.md.get('runs',[])
            if len(runs) == 0: return "None"
            else: return runs[0][0]
        if key == 'subrun_number':
            runs = self.md.get('runs')
            if not runs or len(runs[0]) != 3 : return "None"
            else: return runs[0][1]
        if key == 'run_type':
            runs = self.md.get('runs',[])
            if len(runs) == 0: return "None"
            else: return runs[0][-1] # the type is the last field
        if key == 'app_name':
            return self.md.get('application',{}).get('name','None')
        if key == 'app_family':
            return self.md.get('application',{}).get('family','None')
        if key == 'app_version':
            return self.md.get('application',{}).get('version','None')
        if key == 'year':
            return '%04d' % self.date.year
        elif key == 'month':
            return '%02d' % self.date.month
        elif key == 'day':
            return '%02d' % self.date.day
        else:
            return str(self.md[key])

def format_path_needs_metadata(path_template):
    """ Returns True if the template needs the file metadata,
    False if it only contains keys that don't depend on the metadata """

    template = _MDTemplate(path_template)
    try:
        template.substitute( { 'srcpath' : '',
                              'basepath' : '',
                              'relpath' : '',
                            } )
        return False
    except KeyError:
        return True
    except ValueError as ex:
        raise SyntaxError("Invalid path template: %s: %s" % (path_template, ex))

def format_path(path_template, metadata, mtime, srcdir=None, basedir=None):
    
    template = _MDTemplate(path_template)
    try:
        mapping = _MDMapping(metadata, mtime, srcdir, basedir)
        return os.path.normpath(template.substitute(mapping))
    except (KeyError, ValueError) as ex:
        raise SyntaxError("Invalid path template: %s: %s" % (path_template, ex))

if __name__ == '__main__':
    import time
    template = '/test/path/${year}/${month}/${day}/${runnumber/1000000[=2]}/${Runnumber/10000[=2]}/${runnumber/100[=2]}/${runnumber[=2]}/${missing[3]}'
    metadata = CaseInsensitiveDict({ 'Runnumber' : '123456' })
    print(format_path(template, metadata, time.time()))
    
    template = '/test/path/${year}/${month}/${day}/${runnumber/100[6]}/${runnumber%100}/${missing[3]}'
    metadata = CaseInsensitiveDict({ 'runnumber' : '123456' })
    print(format_path(template, metadata, time.time()))
    
    template = '/test/path/${year}/${month}/${day}/${runnumber/100[6]}/${runnumber[=2]}/${missing[3]}'
    metadata = CaseInsensitiveDict({ 'runnumber' : '123456' })
    print(format_path(template, metadata, time.time()))

    template = '/test/path/${year}/${month}/${day}/${runnumber[8/2]}/${missing[3]}'
    metadata = CaseInsensitiveDict({ 'runnumber' : '123456' })
    print(format_path(template, metadata, time.time()))

    template = '/test/path/${year}/${month}/${day}/${runnumber[6/2]}/${missing[3]}'
    metadata = CaseInsensitiveDict({ 'runnumber' : '0123456' })
    print(format_path(template, metadata, time.time()))
    template = '/test/path/${year}/${month}/${day}/${runnumber[6/2]}/${missing[3]}'
    metadata = CaseInsensitiveDict({ 'runnumber' : '1123456' })
    print(format_path(template, metadata, time.time()))

    template = '/test/path/${run_number}/${subrun_number}/${run_type}'
    metadata = CaseInsensitiveDict({ 'runs' : [] })
    print(format_path(template, metadata, time.time()))

    metadata = CaseInsensitiveDict({ 'runs' : [ [123456, "run_type"] ] })
    print(format_path(template, metadata, time.time()))

    metadata = CaseInsensitiveDict({ 'runs' : [ [123456, 78, "run_type"] ] })
    print(format_path(template, metadata, time.time()))

    print(format_path('/some/path', metadata, time.time(), '/original/path/to/dir', '/original'))
    print(format_path('${srcpath}', metadata, time.time(), '/original/path/to/dir', '/original'))
    print(format_path('${basepath}', metadata, time.time(), '/original/path/to/dir', '/original'))
    print(format_path('/newpath/${relpath}', metadata, time.time(), '/original/path/to/dir', '/original'))
    print(format_path('/newpath/${relpath}', metadata, time.time(), '/original/path/to/dir'))
