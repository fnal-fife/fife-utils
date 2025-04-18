#!/bin/sh

# find ourselves...
case x$0 in
x/*) prefix=$(dirname $0) ;;
x./*) prefix=$(dirname $PWD/$0) ; prefix=$(dirname $prefix);;
x*)  prefix=$(dirname $PWD/$0) ;;
esac
prefix=$(dirname $prefix)

. unittest.bash

# setup dependencies
spack load --first ifdhc@2.7.4  os=default_os
spack load --first sam-web-client@3.6 os=default_os
export ifdh_load="2.7.4 os=default_os"
# add our path and pythnpath entries
export PATH=$prefix/bin:$PATH
export PYTHONPATH=$prefix/lib:$PYTHONPATH
export EXPERIMENT=samdev
# export X509_USER_PROXY=$(ifdh getProxy)
export BEARER_TOKEN_FILE=$(ifdh getToken)

printenv | grep TOKEN


case "x$1" in
x--python3) export p3=true; shift;;
*)          export p3=false;;
esac

setup_proj() {
    echo "setup_proj: starting" >&3
    export SAM_PROJECT=mwm_`date +%Y%m%d%H%M%S`_$$
    export IFDH_CP_MAXRETRIES=0    

    exp=hypot
    htgettoken -i hypot -a htvaultprod.fnal.gov
    cpurl=`ifdh startProject $SAM_PROJECT  samdev gen_cfg  mengel samdev `
    outdir=/tmp/out$$
    echo "setup_proj: started $SAM_PROJECT url $cpurl" >&3
    workdir=/pnfs/${exp}/scratch/users/$USER/fife_utils_test/work.$$
}

end_proj() {
    ifdh endProject $cpurl
    ifdh cleanup
}

test_ucondb_path() {
    export POMS_TASK_ID=9999
    rm -rf $workdir
    mkdir $workdir  && chmod g+wx $workdir
    ../libexec/fife_wrap --debug --source /cvmfs/fermilab.opensciencegrid.org/packages/common/setup-env.sh --dry_run --spack-load '--only dependencies fife-utils@3.7.3' --limit 1 --multifile --appname demo --appfamily demo --appvers demo  --exe cat --addoutput bar.root --rename unique --declare_metadata --add_to_dataset _poms_task --add_location --dest 'https://dbdata0vm.fnal.gov:9443/mu2e_ucondb_prod/app/data/mwmtest' -- '>bar.root' '<' 
}

test_client_1() {
    export POMS_TASK_ID=9999
    rm -rf $workdir
    mkdir $workdir  && chmod g+wx $workdir
    ../libexec/fife_wrap --debug --source /cvmfs/fermilab.opensciencegrid.org/packages/common/setup-env.sh --spack-load '--only dependencies fife-utils@3.7.3' --limit 1 --multifile --appname demo --appfamily demo --appvers demo  --exe cat --addoutput bar.root --rename unique --declare_metadata --add_to_dataset _poms_task --add_location --dest $workdir -- '>bar.root' '<' 
}

test_client_2() {
    export POMS_TASK_ID=9999
    rm -rf $workdir
    mkdir $workdir  && chmod g+wx $workdir
    ../libexec/fife_wrap --debug --source /cvmfs/fermilab.opensciencegrid.org/packages/common/setup-env.sh --spack-load '--only dependencies fife-utils@3.7.3' --prescript-unquote 'for%20i%20in%201%202%203%3B%20do%20echo%20%24i%20%3B%20done' --limit 1 --multifile --appname demo --appfamily demo --appvers demo  --exe cat --addoutput bar.root --rename unique --declare_metadata --add_to_dataset _poms_task --add_location --dest $workdir -- '>bar.root' '<' 
}
test_client_2a() {
    export POMS_TASK_ID=9999
    rm -rf $workdir
    mkdir $workdir  && chmod g+wx $workdir
    ../libexec/fife_wrap --debug --source /cvmfs/fermilab.opensciencegrid.org/packages/common/setup-env.sh --spack-load '--only dependencies fife-utils@3.7.3' --prescript-unquote 'for%20i%20in%201%202%203%3B%20do%20echo%20%24i%20%3B%20done' --limit 1 --multifile --appname demo --appfamily demo --appvers demo  --exe cat --addoutput bar.root --rename unique --declare_metadata --add_to_dataset _poms_analysis --add_location --dest $workdir -- '>bar.root' '<' 
}
test_client_3() {
    export POMS_TASK_ID=9999
    rm -rf $workdir
    mkdir $workdir  && chmod g+wx $workdir
    ../libexec/fife_wrap --debug --source /cvmfs/fermilab.opensciencegrid.org/packages/common/setup-env.sh --spack-load '--only dependencies fife-utils@3.7.3' --prescript-unquote 'for%20i%20in%201%202%203%3B%20do%20echo%20%24i%20%3B%20done' --limit 1 --multifile --appname demo --appfamily demo --appvers demo  --exe /bin/false --exe_stdout0=bar.root --addoutput bar.root --rename unique --declare_metadata --add_to_dataset _poms_task --add_location --dest $workdir 
    echo exit status $?
}

test_client_tmpl() {
    export POMS_TASK_ID=9999
    rm -rf $workdir
    mkdir $workdir  && chmod g+wx $workdir
    ../libexec/fife_wrap --debug --source /cvmfs/fermilab.opensciencegrid.org/packages/common/setup-env.sh --spack-load '--only dependencies fife-utils@3.7.3' --limit 1 --multifile --appname demo --appfamily demo --appvers demo  --exe cat --addoutput bar.root --rename unique --declare_metadata --add_to_dataset _poms_task --add_location --dest $workdir -- '>bar.root' '<' 
}
test_client_4() {
    export POMS_TASK_ID=9999
    rm -rf $workdir
    mkdir $workdir  && chmod g+wx $workdir
    ../libexec/fife_wrap --debug --source /cvmfs/fermilab.opensciencegrid.org/packages/common/setup-env.sh --spack-load '--only dependencies fife-utils@3.7.3' --limit 4 --multifile --appname demo --appfamily demo --appvers demo  --exe cat --addoutput bar.root --rename unique --declare_metadata --add_location --add_to_dataset _poms_task --dest $workdir -- '>bar.root' '<' 
}

test_pre_post_1() {
    export POMS_TASK_ID=9999
    rm -rf $workdir
    mkdir $workdir  && chmod g+wx $workdir
    ../libexec/fife_wrap --debug --source /cvmfs/fermilab.opensciencegrid.org/packages/common/setup-env.sh --spack-load '--only dependencies fife-utils@3.7.3' --limit 4 --multifile --appname demo --appfamily demo --appvers demo  --prescript-unquote="echo%20before" --exe cat --postscript-unquote="echo%20after" --  '<' 
}


test_client_excl() {
    export POMS_TASK_ID=9999
    rm -rf $workdir
    mkdir $workdir  && chmod g+wx $workdir
    ../libexec/fife_wrap --debug --source /cvmfs/fermilab.opensciencegrid.org/packages/common/setup-env.sh --spack-load '--only dependencies fife-utils@3.7.3' --limit 1 --multifile --appname demo --appfamily demo --appvers demo  --exe cat --addoutput bar.root --rename unique --declare_metadata --add_location --add_to_dataset _poms_task --dataset_exclude '*.xyzzy' --dest $workdir -- '>bar.root' '<' 
}

test_env_meta() {
   export POMS_TASK_ID=9999
   export HYPOTCODE_FAST=1
    rm -rf $workdir
    mkdir $workdir  && chmod g+wx $workdir
   ../libexec/fife_wrap --debug --source /cvmfs/fermilab.opensciencegrid.org/packages/common/setup-env.sh --spack-load hypotcode@2.1 --spack-load '--only dependencies fife-utils@3.7.3' --spack-load "ifdhc@$ifdh_load" --limit 4 --multifile --appname demo --appfamily demo --appvers demo --exe hypot.exe --postscript 'export INTENSITY=12.5' --addoutput foo.txt --rename unique --declare_metadata --add_metadata secondary.intensity=\$INTENSITY --add_location --add_to_dataset _poms_task --dest $workdir -- -o foo.txt -c
}

test_hash_dir() {
    export POMS_TASK_ID=9999
    mkdir /tmp/hashdir$$
    rm -rf $workdir
    mkdir $workdir  && chmod g+wx $workdir
    ../libexec/fife_wrap --debug --export-unquote FOO%3d%22%27bar%27_%27baz%27%22 --source /cvmfs/fermilab.opensciencegrid.org/packages/common/setup-env.sh --limit 1 --multifile --appname demo --appfamily demo --appvers v1_0  --exe cat --addoutput bar.root --rename unique --declare_metadata --add_location --add_to_dataset _poms_task --dataset_exclude '*.xyzzy' --dest /tmp/hashdir$$ --hash 2 -- '>bar.root' '<' 
  res=$?
  ls -lR /tmp/hashdir$$
  return $res
}

test_hash_dir_sha() {
    export POMS_TASK_ID=9999
    rm -rf $workdir
    mkdir $workdir  && chmod g+wx $workdir
    ../libexec/fife_wrap --debug --export-unquote FOO%3d%22%27bar%27_%27baz%27%22 --source /cvmfs/fermilab.opensciencegrid.org/packages/common/setup-env.sh --spack-load '--only dependencies fife-utils@3.7.3' --limit 1 --multifile --appname demo --appfamily demo --appvers v1_0  --exe cat --addoutput bar.root --rename unique --declare_metadata --add_location --add_to_dataset _poms_task --dataset_exclude '*.xyzzy' --dest $workdir --hash 2 --hash_alg sha256 -- '>bar.root' '<' 
}

test_parallel() {
    export POMS_TASK_ID=9999
    rm -rf $workdir
    mkdir $workdir  && chmod g+wx $workdir
    mkdir /tmp/out$$
    ../libexec/fife_wrap --debug --limit 4 --multifile --appname demo --appfamily demo --appvers v1_0  --exe cat --addoutput 'bar*.root' --rename unique --declare_metadata --add_location --parallel=2 --dest=/tmp/out$$ -- '>bar${nthfile}.root' '<' 
    res=$?
    ls -Rl /tmp/out$$
    rm -rf /tmp/out$$
    return $res
}

test_multi_format_path() {
    export POMS_TASK_ID=9999
    rm -rf $workdir
    mkdir $workdir  && chmod g+wx $workdir
    mkdir /tmp/out$$
    ../libexec/fife_wrap --debug --limit 4 --multifile --appname demo --appfamily demo --appvers v1_0  --exe cat --addoutput 'bar*.root' --rename unique --declare_metadata --add_location --dest=/tmp/out$$/'${file_name[=7]}' -- '>bar${nthfile}.root' '<' 
    res=$?
    ls -Rl /tmp/out$$
    rm -rf /tmp/out$$
    return $res
}
test_parallel_hashdir_lots() {
    export POMS_TASK_ID=9999
    rm -rf $workdir
    mkdir $workdir  && chmod g+wx $workdir
    mkdir /tmp/out$$
    for i in 0 1 2 3 4 5 6 7 8 9; do for j in 0 1 2 3 4 5 6 7 8 9; do echo $i$j > bar$i.$j.root; done; done
    ../libexec/fife_wrap --debug --limit 5 --multifile --appname demo --appfamily demo --appvers v1_0  --exe cat --addoutput 'bar*.root' --rename unique --declare_metadata --add_location --parallel=2 --hash 2 --hash_alg sha256 --dest=/tmp/out$$ -- '>bar${nthfile}.root' '<' 
    res=$?
    ls -Rl /tmp/out$$
    rm -rf /tmp/out$$
    return $res
}


test_quot_env() {
    export POMS_TASK_ID=9999
    rm -rf $workdir
    mkdir $workdir  && chmod g+wx $workdir
    ../libexec/fife_wrap --debug --export-unquote FOO%3d%60bar%60_%60baz%60 --source /cvmfs/fermilab.opensciencegrid.org/packages/common/setup-env.sh --spack-load '--only dependencies fife-utils@3.7.3' --limit 4 --multifile --appname demo --appfamily demo --appvers v1_0  --exe cat --addoutput bar.root --rename unique --declare_metadata --add_location --add_to_dataset _poms_task --dataset_exclude '*.xyzzy' --dest $workdir --hash 2 -- '>bar.root' '<'  | tee tqe.out
}



if [ ! -r /tmp/did_gen_cfg ]
then
    $HYPOTCODE_DIR/bin/rebuild_gen_cfg
    touch /tmp/did_gen_cfg
fi

printf "python: "
which python
ups active

# dropped test_parallel_hashdir_lots , takes too long
#testsuite fife_wrap_tests -s setup_proj -t end_proj test_quot_env

testsuite fife_wrap_tests -s setup_proj -t end_proj test_pre_post_1 test_env_meta test_client_tmpl test_client_1 test_client_2 test_client_2a test_client_3 test_client_4 test_client_excl test_hash_dir test_hash_dir_sha test_quot_env test_multi_format_path test_ucondb_path test_parallel 

fife_wrap_tests "$@"
