#!/bin/bash
nohup bash -c "
(
    bash t1.bash -v
    bash t1.bash --python3 -v
    bash test_fife_wrap.sh -v 2>&1
    bash test_fife_wrap.sh --python3 -v 2>&1
    python test_mu2e_sam_prefix.py
)" < /dev/null > test_suite.out 2>&1 &
echo "running as $&"
sleep 1
tail -f test_suite.out
