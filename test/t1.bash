#!/bin/bash

. ./unittest.bash

case "x$1" in
x--python3) export p3=true; shift;;
*)          export p3=false;;
esac

if $p3
then
    test -d /tmp/py3 || mkdir /tmp/py3
    ln -s /bin/python3 /tmp/py3/python
    PATH=/tmp/py3:$PATH
    . `ups unsetup python_future_six_request`
    . `ups setup ifdhc v2_6_1 -q python36`
fi

count_report_files() {
    echo
    echo "$1 $dataset:"
    echo "----------------------------------"
    ifdh translateConstraints "defname: $dataset"  | tee /tmp/v$$
    ifdh locateFiles  `cat /tmp/v$$` | tee /tmp/w$$
    echo "----------------------------------" 
    eval "$2=`cat /tmp/w$$ | grep '^	' |  wc -l`"
    rm /tmp/v$$
    eval echo "count: \$$2"
}

setup_tests() {
   export EXPERIMENT=samdev
   export SAM_EXPERIMENT=samdev
   #export SAM_STATION=samdev-test
   export SAM_STATION=samdev
   export IFDH_BASE_URI="http://samweb.fnal.gov:8480/sam/samdev/api"
   export IFDH_CP_MAXRETRIES=0

   # pick an experiment pnfs area from whats available
   for e in nova dune uboone minerva
   do
       if [ -d /pnfs/$e ]
       then
           exp=$e
           break
       fi
   done

   workdir=/pnfs/${exp}/scratch/users/$USER/fife_utils_test/work.$$
   if [ ! -r $workdir ]
   then
       mkdir -p $workdir
   fi
   cd $workdir
   if [ ! -r dataset ] 
   then
       echo "testds_`hostname --fqdn`_`date +%s`_$$" > dataset
   fi
   pnfs_dir=/pnfs/${exp}/scratch/users/$USER/fife_util_test 
   ifdh ls $pnfs_dir || ifdh mkdir $pnfs_dir || true
   read dataset < dataset
   export dataset
   export X509_USER_PROXY=/tmp/x509up_u`id -u`
   rm -f $X509_USER_PROXY
   kx509
   voms-proxy-init -rfc -noregen -debug -voms fermilab:/fermilab/nova/Role=Analysis
   export IFDH_NO_PROXY=1
   # now in table file...
   export X509_USER_PROXY=/tmp/x509up_u`id -u`
   export SSL_CERT_DIR=/etc/grid-security/certificates
   export CPN_DIR=/no/such/dir
}

test_add_dataset_flist_norename() {
   rm -rf data
   mkdir data
   rm -f file_list
   for i in 1 2 3 
   do
       fname="f${i}_$$.txt"
       echo "file $i" > data/$fname
       echo `pwd`/data/$fname 
   done > file_list

   rm -f meta.json
   cat > meta.json <<EOF
{
 "file_type": "test", 
 "file_format": "data", 
 "content_status": "good", 
 "group": "samdev", 
 "data_tier": "log", 
 "application": {
  "family": "test", 
  "name": "test", 
  "version": "1"
 } 
}
EOF

   sam_add_dataset --no-rename -e $EXPERIMENT --file file_list --metadata meta.json --name ${dataset}

   echo "dataset $dataset contains:" 
   ifdh translateConstraints "defname: $dataset" 
   sleep 20 
}

test_add_dataset_flist() {
   rm -rf data
   mkdir data
   rm -f file_list
   for i in 1 2 3 
   do
       fname="f${i}.txt"
       echo "file $i" > data/$fname
       echo `pwd`/data/$fname 
   done > file_list

   rm -f meta.json
   cat > meta.json <<EOF
{
 "file_type": "test", 
 "file_format": "data", 
 "content_status": "good", 
 "group": "samdev", 
 "data_tier": "log", 
 "application": {
  "family": "test", 
  "name": "test", 
  "version": "1"
 } 
}
EOF

   sam_add_dataset -e $EXPERIMENT --file file_list --metadata meta.json --name ${dataset}

   echo "dataset $dataset contains:" 
   ifdh translateConstraints "defname: $dataset" 
   sleep 20 
}

test_add_dataset_flist_glob() {
   mkdir data
   rm -f file_list
   for i in 1 2 3 
   do
       fname="f${i}.txt"
       echo "file $i" > data/$fname
   done
   echo `pwd`/data/f*.txt >  file_list

   rm -f meta.json
   cat > meta.json <<EOF
{
 "file_type": "test", 
 "file_format": "data", 
 "content_status": "good", 
 "group": "samdev", 
 "data_tier": "log", 
 "application": {
  "family": "test", 
  "name": "test", 
  "version": "1"
 } 
}
EOF

   sam_add_dataset -e $EXPERIMENT --directory `pwd`/data --metadata meta.json --name ${dataset}
   #sam_add_dataset -e $EXPERIMENT `pwd`/data meta.json ${dataset}

   echo "dataset $dataset contains:" 
   ifdh translateConstraints "defname: $dataset" 
   sleep 20 
}

test_add_dataset_directory() {
   rm -rf data
   mkdir data
   for i in 1 2 3 
   do
       fname="f${i}.txt"
       echo "file $i" > data/$fname
   done

   rm -f meta.json
   cat > meta.json <<EOF
{
 "file_type": "test", 
 "file_format": "data", 
 "content_status": "good", 
 "group": "samdev", 
 "data_tier": "log", 
 "application": {
  "family": "test", 
  "name": "test", 
  "version": "1"
 } 
}
EOF

   sam_add_dataset -e $EXPERIMENT --directory `pwd`/data --metadata `pwd`/meta.json --name ${dataset}

   echo "dataset $dataset contains:" 
   ifdh translateConstraints "defname: $dataset" 
}

#
# XXX later this should use the end-user command, we're just
# doing an approximation here...
#

add_dataset() {

   echo "workdir is $workdir, in:"
   pwd

   for i in 1 2 3 4 5 6 7 8 9
   do
       fname="${dataset}_f${i}"
       rm -f $fname
       echo "file $i" > $fname
       checksum=`ifdh checksum $fname 2>/dev/null`
       size=`cat $fname | wc -c`
       rm -f $fname.json 
       cat > $fname.json <<EOF
{
 "file_name": "$fname", 
 "file_type": "test", 
 "file_format": "data", 
 "file_size": $size, 
 "checksum": $checksum,
 "content_status": "good", 
 "group": "samdev", 
 "data_tier": "log", 
 "application": {
  "family": "test", 
  "name": "test", 
  "version": "1"
 } 
}
EOF
       case `pwd` in
       /pnfs/*/scratch/*) location="dcache:`pwd`";;
       /pnfs/*) location="enstore:`pwd`";;
       /grid/*) location="${EXPERIMENT}data:`pwd`";;
       /nova/*) location="samdevdata:`pwd`";;
       esac

       samweb declare-file $fname.json
       samweb add-file-location $fname $location
   done
   samweb create-definition $dataset "file_name like '${dataset}_f%'"

   count_report_files "add_dataset:" count
   # sometimes the last file we made isn't visible right away
   # through a different DCache door.  Wait a little for it to settle out...
   sleep 20 
}

test_validate_1() {
    ls -l $workdir
    sam_validate_dataset --name $dataset
}

test_audit_1() {
    ls -l $workdir
    sam_audit_dataset --name $dataset --dest=$workdir
}

test_audit_2() {
    ls -l $workdir
    sam_audit_dataset --name $dataset --dest=$workdir | tee /tmp/sadout$$
    echo "------"
    grep "Present and declared: 9" /tmp/sadout$$
}

test_audit_3() {
    mkdir $workdir/hide
    mv *_f2 $workdir/hide
    ls -l $workdir
    sam_audit_dataset --name $dataset --dest=$workdir | tee /tmp/sadout$$
    mv $workdir/hide/*_f2 .
    echo "------"
    grep "Present and declared: 8" /tmp/sadout$$ &&
        grep "Present at wrong location: 1" /tmp/sadout$$ 
}


test_validate_2() {
    ls -l $workdir
    rm -f ${dataset}_f2_hide
    mv ${dataset}_f2 ${dataset}_f2_hide
    if sam_validate_dataset --name $dataset
    then
        res=1
    else
        res=0
    fi
    mv ${dataset}_f2_hide ${dataset}_f2
    return $res
}

test_validate_prune() {
    ls -l $workdir
    mv ${dataset}_f2 ${dataset}_f2_hide
    sam_validate_dataset --name $dataset --prune 2>&1 | tee /tmp/out$$
    grep '_f2 has 0 locations' /tmp/out$$
    res=$?
    mv ${dataset}_f2_hide ${dataset}_f2
    return $res
}

test_validate_locality() {
    sam_clone_dataset -v -b 2 --name $dataset --dest $pnfs_dir
    sam_validate_dataset -v --name $dataset --locality > out
    sam_unclone_dataset -v -b 2 --name $dataset --dest $pnfs_dir
    grep Locality out
}

test_clone() {
    count_report_files "before:" locs1
    sam_clone_dataset -v -b 2 --name $dataset --dest $pnfs_dir
    count_report_files "after:" locs2
    [ "$locs2" -gt "$locs1" ]
}

test_clone_n() {
    count_report_files "before:" locs1
    sam_clone_dataset -v -N 3 -b 2 --name $dataset --dest $pnfs_dir
    count_report_files "after:" locs2
    [ "$locs2" -gt "$locs1" ]
}

test_copy2scratch_dataset() {
    count_report_files "before:" locs1
    sam_copy2scratch_dataset -v --name $dataset --dest $pnfs_dir
    count_report_files "after:" locs2
    [ "$locs2" -gt "$locs1" ]
}

test_move2archive() {
    count_report_files "before:" locs1
    sam_move2archive_dataset -v --name $dataset --dest $pnfs_dir
    count_report_files "after:" locs2
    [ "$locs2" -eq "$locs1" ]
}

test_move2archive_double() {
    count_report_files "before:" locs1

    # make a *second* copy
    ifdh mkdir ${pnfs_dir}_1
    sam_clone_dataset --name $dataset --dest ${pnfs_dir}_1
    count_report_files "after clone:" locs2
    
    # move to third place
    ifdh mkdir ${pnfs_dir}_2
    sam_move2archive_dataset -v --name $dataset --dest ${pnfs_dir}_2
    count_report_files "after archive:" locs3
   
    # should be back to first count
    echo counts $locs1 $locs2 $locs3
    [ "$locs2" -gt "$locs1"  -a "$locs3" -eq "$locs1" ]
}

test_move2persistent() {
    count_report_files "before:" locs1
    sam_move2persistent_dataset -v --name $dataset --dest $pnfs_dir
    count_report_files "after:" locs2
    [ "$locs2" -eq "$locs1" ]
}

test_archive_dataset() {
    count_report_files "before:" locs1
    # use a non-archving location for testing!
    sam_archive_dataset -v --name $dataset --dest $pnfs_dir
    count_report_files "after:" locs2
    [ "$locs2" -eq "$locs1" ]
}

test_unclone() {
    count_report_files "before:" locs1
    sam_clone_dataset -v --name $dataset --dest $pnfs_dir
    count_report_files "after clone:" locs2
    sam_unclone_dataset -v --name $dataset --dest $pnfs_dir/
    count_report_files "after unclone:" locs3
    [ "$locs2" -gt "$locs1" -a "$locs3" -lt "$locs2" -a "$locs1" -eq "$locs3" ]
}

test_unclone_slashes() {
    # same as the other, but extra slashes in unclone --dest param
    count_report_files "before:" locs1
    sam_clone_dataset -v --name $dataset --dest $pnfs_dir
    count_report_files "after:" locs2
    sdest=`echo $pnfs_dir | sed -e 's|\([^s]/\)|\1/|'`
    sam_unclone_dataset --name $dataset --dest $sdest
    count_report_files "after unclone:" locs3
    [ "$locs2" -gt "$locs1" -a "$locs3" -lt "$locs2" -a "$locs1" -eq "$locs3" ]
}

test_modify() {
    cat >> foo.json <<EOF
  {
    "Quality.overall": "questionable"
  }
EOF
    sam_modify_dataset_metadata --name $dataset --metadata foo.json
    f=`ifdh translateConstraints "defname:$dataset" | head -1`
    ifdh getMetadata $f | grep Quality.overall
}

test_pin() {
    sam_pin_dataset --name $dataset
}

test_split_clone() {
    proj="${USER}_clonetest_$$_`date +%s`"
    count_report_files "before:" locs1
    echo "----------------------------------"
    sam_clone_dataset -v --project=${proj} --just-start-project -b 2 --name $dataset --dest $pnfs_dir
    echo "----------------------------------"
    sam_clone_dataset -v --project=${proj} --connect-project    -b 2 --name $dataset --dest $pnfs_dir
    echo "----------------------------------"
    count_report_files "after:" locs2
    [ "$locs2" -gt "$locs1" ]
}


test_retire() {
    list=`sam_validate_dataset -v --name $dataset | grep located`
    sam_retire_dataset -v --name $dataset
    echo "$list" | (
        fails=0
        while read loc path
        do
            path=`echo $path | sed -e 's/[a-z]*://'`
            if [ -z "$path" ]
            then
               continue
            fi
            if [ -r $path ] 
            then
                echo Ouch -- $path still there
                fails=$(( $fails + 1))
            fi
        done
        [ $fails = 0 ]
        )
}

test_archive_restore_dir() {
    cd $workdir
    mkdir mytestdir
    for f in a b c d; do echo foo $f >> mytestdir/$f; done
    sam_archive_directory_image --dest=$pnfs_dir --src=$workdir/mytestdir
    rm -rf reftestdir
    mv mytestdir reftestdir
    listout=`sam_restore_directory_image --list | grep $workdir/mytestdir | tail -1`
    set : $listout
    sam_restore_directory_image --restore=`pwd`/mytestdir --date=$2
    diff --recursive mytestdir reftestdir && [ "x$listout" != "x" ]
}

testsuite test_utils \
	-s setup_tests \
        add_dataset \
        test_unclone \
        test_unclone_slashes \
        test_retire \
        add_dataset \
	test_validate_prune \
        test_retire \
        test_archive_restore_dir \
        add_dataset \
	test_validate_1 \
	test_audit_1 \
	test_audit_2 \
	test_audit_3 \
	test_validate_2 \
        test_modify \
	test_clone  \
        test_retire \
        add_dataset \
	test_validate_1 \
	test_clone_n  \
        test_retire \
        test_add_dataset_flist \
	test_validate_1 \
        test_retire \
        test_add_dataset_flist_norename \
	test_validate_1 \
        test_retire \
        test_add_dataset_flist_glob \
	test_validate_1 \
        test_retire \
        test_add_dataset_directory \
	test_validate_1 \
        test_retire  \
        add_dataset \
        test_copy2scratch_dataset \
        test_retire \
        add_dataset \
        test_archive_dataset \
        test_retire \
        add_dataset \
        test_move2persistent \
        test_retire \
        add_dataset \
        test_move2archive \
        test_retire \
        add_dataset \
        test_move2archive_double \
        test_retire \

         
test_utils "$@"
