#!/bin/sh

. unittest.bash

setup_proj() {
    export EXPERIMENT=samdev
   
    count=${count:-0} + 1
    export SAM_PROJECT=mwm_`date +%Y%m%d%H%M`_$$_$count
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
    ../libexec/fife_wrap --debug --find_setups --setup fife_utils --limit 1 --multifile  --exe cat --addoutput bar.root --rename unique --declare_metadata --add_to_dataset _poms_task --add_location --dest /pnfs/nova/scratch/users/mengel/ -- '>bar.root' '<' 
}

test_client_2() {
    export POMS_TASK_ID=9999
    ../libexec/fife_wrap --debug --find_setups --setup fife_utils --prescript-unquote 'for%20i%20in%201%202%203%3B%20do%20echo%20%24i%20%3B%20done' --limit 1 --multifile  --exe cat --addoutput bar.root --rename unique --declare_metadata --add_to_dataset _poms_task --add_location --dest /pnfs/nova/scratch/users/mengel/ -- '>bar.root' '<' 
}
test_client_2a() {
    export POMS_TASK_ID=9999
    ../libexec/fife_wrap --debug --find_setups --setup fife_utils --prescript-unquote 'for%20i%20in%201%202%203%3B%20do%20echo%20%24i%20%3B%20done' --limit 1 --multifile  --exe cat --addoutput bar.root --rename unique --declare_metadata --add_to_dataset _poms_analysis --add_location --dest /pnfs/nova/scratch/users/mengel/ -- '>bar.root' '<' 
}
test_client_3() {
    export POMS_TASK_ID=9999
    ../libexec/fife_wrap --debug --find_setups --setup fife_utils --prescript-unquote 'for%20i%20in%201%202%203%3B%20do%20echo%20%24i%20%3B%20done' --limit 1 --multifile  --exe /bin/false --exe_stdout0=bar.root --addoutput bar.root --rename unique --declare_metadata --add_to_dataset _poms_task --add_location --dest /pnfs/nova/scratch/users/mengel/ 
    echo exit status $?
}

test_client_tmpl() {
    export POMS_TASK_ID=9999
    ../libexec/fife_wrap --debug --find_setups --setup fife_utils --limit 1 --multifile  --exe cat --addoutput bar.root --rename unique --declare_metadata --add_to_dataset _poms_task --add_location --dest '/pnfs/nova/scratch/users/mengel/${month}/' -- '>bar.root' '<' 
}
test_client_4() {
    export POMS_TASK_ID=9999
    ../libexec/fife_wrap --debug --find_setups --setup fife_utils --limit 4 --multifile  --exe cat --addoutput bar.root --rename unique --declare_metadata --add_location --add_to_dataset _poms_task --dest /pnfs/nova/scratch/users/mengel -- '>bar.root' '<' 
}
test_client_excl() {
    export POMS_TASK_ID=9999
    ../libexec/fife_wrap --debug --find_setups --setup fife_utils --limit 1 --multifile  --exe cat --addoutput bar.root --rename unique --declare_metadata --add_location --add_to_dataset _poms_task --dataset_exclude '*.xyzzy' --dest /pnfs/nova/scratch/users/mengel -- '>bar.root' '<' 
}

test_env_meta() {
   export POMS_TASK_ID=9999
   export HYPOTCODE_FAST=1
   ../libexec/fife_wrap --debug --find_setups --setup hypotcode --setup fife_utils --limit 4 --multifile --exe hypot.exe --postscript 'export INTENSITY=12.5' --addoutput foo.txt --rename unique --declare_metadata --add_metadata secondary.intensity=\$INTENSITY --add_location --add_to_dataset _poms_task --dest /pnfs/nova/scratch/users/mengel -- -o foo.txt -c
}

testsuite fife_wrap_tests -s setup_proj -t end_proj test_env_meta test_client_tmpl test_client_1 test_client_2 test_client_2a test_client_3 test_client_4 test_client_excl

fife_wrap_tests "$@"
