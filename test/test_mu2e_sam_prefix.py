import os
import re

os.system("ln -s ../libexec/fife_wrap ./fife_wrap.py")
import fife_wrap

class fake_opts:
    def __init__(self):
        self.debug = True

os.environ["EXPERIMENT"] = "mu2e"
w = fife_wrap.Wrapper()
w.options = fake_opts()
dst = "https://dbdata0vm.fnal.gov:9443/mu2e_ucondb_prod/app/data/mdc2020o_v3/"
dst = re.sub(w.urlstart_re,"", dst)
print( "prefix:", w.sam_prefix(dst))
