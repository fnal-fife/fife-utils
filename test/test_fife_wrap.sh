#!/bin/sh

. unittest.bash

setup_proj() {
    export EXPERIMENT=samdev
   
    count=${count:-0} + 1
    export SAM_PROJECT=mwm_`date +%Y%m%d%H`_$$_$count
    sleep 1
    cpurl=`ifdh startProject $SAM_PROJECT  samdev gen_cfg  mengel samdev `
    outdir=/tmp/out$$
}

end_proj() {
    ifdh endProject $cpurl
    ifdh cleanup
}

test_client_1() {
    export POMS_TASK_ID=9999
    ../libexec/fife_wrap --debug --find_setups --setup fife_utils --limit 1 --multifile  --exe cat --addoutput bar.root --rename unique --declare_metadata --add_to_dataset _poms_task --add_locations --dest /pnfs/nova/scratch/users/mengel/ -- '>bar.root' '<' 
}

test_client_4() {
    export POMS_TASK_ID=9999
    ../libexec/fife_wrap --debug --find_setups --setup fife_utils --limit 4 --multifile  --exe cat --addoutput bar.root --rename unique --declare_metadata --add_locations --add_to_dataset _poms_task --dest /pnfs/nova/scratch/users/mengel -- '>bar.root' '<' 
}

testsuite fife_wrap_tests -s setup_proj -t unsetup_proj test_client_1 test_client_4 

fife_wrap_tests "$@"
