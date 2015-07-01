. unittest.bash

test_fife_wrap_getconfig() {
    export EXPERIMENT=samdev
    t=`date +%s`
    export SAM_PROJECT=mwm_test_${$}-${t} 
    ifdh startProject $SAM_PROJECT $EXPERIMENT test-project mengel samdev

    fife_wrap --source /grid/fermiapp/common/products/etc/setups --export FOO=bar --exe ./fake_exe.sh --getconfig --limit 3 2>&1 | tee out
    count1=`grep '^fake_exe:' out | wc -l`
    fife_wrap --source /grid/fermiapp/common/products/etc/setups --export FOO=bar --exe ./fake_exe.sh --getconfig --limit 2 2>&1 | tee out
    count2=`grep '^fake_exe:' out | wc -l`
    echo "counts: " $count1 $count2
    [ $count1 = 3 -a $count2 = 2 ]
}

test_fife_wrap_multifile() {
    export EXPERIMENT=samdev
    t=`date +%s`
    export SAM_PROJECT=mwm_test_${$}-${t} 
    ifdh startProject $SAM_PROJECT $EXPERIMENT test-project mengel samdev

    fife_wrap --source /grid/fermiapp/products/common/etc/setups --export FOO=bar --exe ./fake_exe.sh --multifile --config fake.cfg --limit 3 2>&1 | tee out
    count1=`grep '^fake_exe:' out | wc -l`
    fife_wrap --source /grid/fermiapp/products/common/etc/setups --export FOO=bar --exe ./fake_exe.sh --multifile --config fake.config --limit 2 2>&1 | tee out
    count2=`grep '^fake_exe:' out | wc -l`
    echo "counts: " $count1 $count2
    [ $count1 = 3 -a $count2 = 2 ]
}

test_fife_wrap_art() {
    export EXPERIMENT=samdev
    t=`date +%s`
    export SAM_PROJECT=mwm_test_${$}-${t} 
    ifdh startProject $SAM_PROJECT $EXPERIMENT test-project mengel samdev

    fife_wrap --source /grid/fermiapp/products/common/etc/setups --export FOO=bar --exe ./fake_exe.sh --ifdh_art --config fake.cfg --limit 3 2>&1 | tee out
    count1=`grep '^fake_exe:' out | wc -l`
    fife_wrap --source /grid/fermiapp/products/common/etc/setups --export FOO=bar --exe ./fake_exe.sh --ifdh_art --config fake.config --limit 2 2>&1 | tee out
    count2=`grep '^fake_exe:' out | wc -l`
    echo "counts: " $count1 $count2
    [ $count1 = 1 -a $count2 = 1 ]
}


testsuite test_wrap \
   test_fife_wrap_getconfig \
   test_fife_wrap_multifile \
   test_fife_wrap_art

test_wrap "$@"
