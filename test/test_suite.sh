#!/bin/bash

(
    bash t1.bash -v
    bash t1.bash --python3 -v
    bash test_fife_wrap.sh -v
    bash test_fife_wrap.sh --python3 -v
    python test_mu2e_sam_prefix.py
) test_suite.out 2>&1 < /dev/null &
