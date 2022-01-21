#!/bin/sh

. unittest.bash

case "x$1" in
x--python3) export p3=true; shift;;
*)          export p3=false;;
esac

setup_proj() {
    #echo "setup_proj: starting" >&3
    export EXPERIMENT=samdev
   
    export SAM_PROJECT=mwm_`date +%Y%m%d%H%M%S`_$$
    sleep 2
    cpurl=`ifdh startProject $SAM_PROJECT  samdev gen_cfg  mengel samdev `
    outdir=/tmp/out$$
    #echo "setup_proj: started $SAM_PROJECT url $cpurl" >&3
}

end_proj() {
    ifdh endProject $cpurl
    ifdh cleanup
}

test_ucondb_path() {
    export POMS_TASK_ID=9999
    ../libexec/fife_wrap --debug --find_setups --dry_run --setup fife_utils --limit 1 --multifile --appname demo --appfamily demo --appvers demo  --exe cat --addoutput bar.root --rename unique --declare_metadata --add_to_dataset _poms_task --add_location --dest 'https://dbdata0vm.fnal.gov:9443/mu2e_ucondb_prod/app/data/mwmtest' -- '>bar.root' '<' 
}

test_client_1() {
    export POMS_TASK_ID=9999
    ../libexec/fife_wrap --debug --find_setups --setup fife_utils --limit 1 --multifile --appname demo --appfamily demo --appvers demo  --exe cat --addoutput bar.root --rename unique --declare_metadata --add_to_dataset _poms_task --add_location --dest /pnfs/nova/scratch/users/mengel/ -- '>bar.root' '<' 
}

test_client_2() {
    export POMS_TASK_ID=9999
    ../libexec/fife_wrap --debug --find_setups --setup fife_utils --prescript-unquote 'for%20i%20in%201%202%203%3B%20do%20echo%20%24i%20%3B%20done' --limit 1 --multifile --appname demo --appfamily demo --appvers demo  --exe cat --addoutput bar.root --rename unique --declare_metadata --add_to_dataset _poms_task --add_location --dest /pnfs/nova/scratch/users/mengel/ -- '>bar.root' '<' 
}
test_client_2a() {
    export POMS_TASK_ID=9999
    ../libexec/fife_wrap --debug --find_setups --setup fife_utils --prescript-unquote 'for%20i%20in%201%202%203%3B%20do%20echo%20%24i%20%3B%20done' --limit 1 --multifile --appname demo --appfamily demo --appvers demo  --exe cat --addoutput bar.root --rename unique --declare_metadata --add_to_dataset _poms_analysis --add_location --dest /pnfs/nova/scratch/users/mengel/ -- '>bar.root' '<' 
}
test_client_3() {
    export POMS_TASK_ID=9999
    ../libexec/fife_wrap --debug --find_setups --setup fife_utils --prescript-unquote 'for%20i%20in%201%202%203%3B%20do%20echo%20%24i%20%3B%20done' --limit 1 --multifile --appname demo --appfamily demo --appvers demo  --exe /bin/false --exe_stdout0=bar.root --addoutput bar.root --rename unique --declare_metadata --add_to_dataset _poms_task --add_location --dest /pnfs/nova/scratch/users/mengel/ 
    echo exit status $?
}

test_client_tmpl() {
    export POMS_TASK_ID=9999
    ../libexec/fife_wrap --debug --find_setups --setup fife_utils --limit 1 --multifile --appname demo --appfamily demo --appvers demo  --exe cat --addoutput bar.root --rename unique --declare_metadata --add_to_dataset _poms_task --add_location --dest '/pnfs/nova/scratch/users/mengel/${month}/' -- '>bar.root' '<' 
}
test_client_4() {
    export POMS_TASK_ID=9999
    ../libexec/fife_wrap --debug --find_setups --setup fife_utils --limit 4 --multifile --appname demo --appfamily demo --appvers demo  --exe cat --addoutput bar.root --rename unique --declare_metadata --add_location --add_to_dataset _poms_task --dest /pnfs/nova/scratch/users/mengel -- '>bar.root' '<' 
}

test_pre_post_1() {
    export POMS_TASK_ID=9999
    ../libexec/fife_wrap --debug --find_setups --setup fife_utils --limit 4 --multifile --appname demo --appfamily demo --appvers demo  --prescript-unquote="echo%20before" --exe cat --postscript-unquote="echo%20after" --  '<' 
}


test_client_excl() {
    export POMS_TASK_ID=9999
    ../libexec/fife_wrap --debug --find_setups --setup fife_utils --limit 1 --multifile --appname demo --appfamily demo --appvers demo  --exe cat --addoutput bar.root --rename unique --declare_metadata --add_location --add_to_dataset _poms_task --dataset_exclude '*.xyzzy' --dest /pnfs/nova/scratch/users/mengel -- '>bar.root' '<' 
}

test_env_meta() {
   export POMS_TASK_ID=9999
   export HYPOTCODE_FAST=1
   ../libexec/fife_wrap --debug --find_setups --setup hypotcode --setup fife_utils --setup "ifdhc $IFDH_VERSION" --limit 4 --multifile --appname demo --appfamily demo --appvers demo --exe hypot.exe --postscript 'export INTENSITY=12.5' --addoutput foo.txt --rename unique --declare_metadata --add_metadata secondary.intensity=\$INTENSITY --add_location --add_to_dataset _poms_task --dest /pnfs/nova/scratch/users/mengel -- -o foo.txt -c
}

test_hash_dir() {
    export POMS_TASK_ID=9999
    mkdir /tmp/hashdir$$
    ../libexec/fife_wrap --debug --export-unquote FOO%3d%22%27bar%27_%27baz%27%22 --find_setups --limit 1 --multifile --appname demo --appfamily demo --appvers v1_0  --exe cat --addoutput bar.root --rename unique --declare_metadata --add_location --add_to_dataset _poms_task --dataset_exclude '*.xyzzy' --dest /tmp/hashdir$$ --hash 2 -- '>bar.root' '<' 
  res=$?
  ls -lR /tmp/hashdir$$
  return $res
}

test_hash_dir_sha() {
    export POMS_TASK_ID=9999
    ../libexec/fife_wrap --debug --export-unquote FOO%3d%22%27bar%27_%27baz%27%22 --find_setups --setup fife_utils --limit 1 --multifile --appname demo --appfamily demo --appvers v1_0  --exe cat --addoutput bar.root --rename unique --declare_metadata --add_location --add_to_dataset _poms_task --dataset_exclude '*.xyzzy' --dest /pnfs/nova/scratch/users/mengel --hash 2 --hash_alg sha256 -- '>bar.root' '<' 
}

test_parallel() {
    export POMS_TASK_ID=9999
    mkdir /tmp/out$$
    ../libexec/fife_wrap --debug --limit 4 --multifile --appname demo --appfamily demo --appvers v1_0  --exe cat --addoutput 'bar*.root' --rename unique --declare_metadata --add_location --parallel=2 --dest=/tmp/out$$ -- '>bar${nthfile}.root' '<' 
    res=$?
    ls -Rl /tmp/out$$
    rm -rf /tmp/out$$
    return $res
}

test_multi_format_path() {
    export POMS_TASK_ID=9999
    mkdir /tmp/out$$
    ../libexec/fife_wrap --debug --limit 4 --multifile --appname demo --appfamily demo --appvers v1_0  --exe cat --addoutput 'bar*.root' --rename unique --declare_metadata --add_location --dest=/tmp/out$$/'${file_name[=7]}' -- '>bar${nthfile}.root' '<' 
    res=$?
    ls -Rl /tmp/out$$
    rm -rf /tmp/out$$
    return $res
}
test_parallel_hashdir_lots() {
    export POMS_TASK_ID=9999
    mkdir /tmp/out$$
    for i in 0 1 2 3 4 5 6 7 8 9; do for j in 0 1 2 3 4 5 6 7 8 9; do for k in 0 1 2 3 4 5 6 7 8 9; do echo $i$j$k > bar$i.$j.$k.root; done; done; done
    IFDH_CP_MAXRETRIES=0 ../libexec/fife_wrap --debug --limit 5 --multifile --appname demo --appfamily demo --appvers v1_0  --exe cat --addoutput 'bar*.root' --rename unique --declare_metadata --add_location --parallel=2 --hash 2 --hash_alg sha256 --dest=/tmp/out$$ -- '>bar${nthfile}.root' '<' 
    res=$?
    ls -Rl /tmp/out$$
    rm -rf /tmp/out$$
    return $res
}


test_quot_env() {
    export POMS_TASK_ID=9999
    ../libexec/fife_wrap --debug --export-unquote FOO%3d%60bar%60_%60baz%60 --find_setups --setup fife_utils --limit 4 --multifile --appname demo --appfamily demo --appvers v1_0  --exe cat --addoutput bar.root --rename unique --declare_metadata --add_location --add_to_dataset _poms_task --dataset_exclude '*.xyzzy' --dest /pnfs/nova/scratch/users/mengel --hash 2 -- '>bar.root' '<'  | tee tqe.out
}

if p3
then
    test -d /tmp/py3 || mkdir /tmp/py3
    ln -s /bin/python3 /tmp/py3/python
    PATH=/tmp/py3:$PATH
    . `ups unsetup python_future_six_request`
    . `ups setup hypotcode`
    . `ups setup -j ifdhc v2_6_1 -q +python36, ifdhc_config v2_6_1a`
    export IFDH_VERSION="v2_6_1 -q +python36"
else
    . `ups setup ifdhc`
fi

export EXPERIMENT=samdev
export X509_USER_PROXY=`ifdh getProxy`
if [ ! -r /tmp/did_gen_cfg ]
then
    $HYPOTCODE_DIR/bin/rebuild_gen_cfg
    touch /tmp/did_gen_cfg
fi

printf "python: "
which python
ups active

#testsuite fife_wrap_tests -s setup_proj -t end_proj test_env_meta

testsuite fife_wrap_tests -s setup_proj -t end_proj test_parallel_hashdir_lots test_parallel test_pre_post_1 test_env_meta test_client_tmpl test_client_1 test_client_2 test_client_2a test_client_3 test_client_4 test_client_excl test_hash_dir test_hash_dir_sha test_quot_env test_multi_format_path test_ucondb_path

fife_wrap_tests "$@"
