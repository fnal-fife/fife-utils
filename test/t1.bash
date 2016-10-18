#!/bin/bash

. unittest.bash

setup_tests() {
   export EXPERIMENT=samdev
   export SAM_EXPERIMENT=samdev
   #export SAM_STATION=samdev-test
   export SAM_STATION=samdev
   export IFDH_BASE_URI="http://samweb.fnal.gov:8480/sam/samdev/api"
   export IFDH_CP_MAXRETRIES=0

   workdir=/nova/data/$USER/work.$$
   if [ ! -r $workdir ]
   then
       mkdir $workdir
   fi
   cd $workdir
   if [ ! -r dataset ] 
   then
       echo "testds_`hostname --fqdn`_`date +%s`_$$" > dataset
   fi
   pnfs_dir=/pnfs/nova/scratch/users/$USER/fife_util_test 
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

test_add_dataset_flist() {
   mkdir data
   : > file_list
   for i in 1 2 3 
   do
       fname="f${i}.txt"
       echo "file $i" > data/$fname
       echo `pwd`/data/$fname >> file_list
   done

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

   sam_add_dataset --cert=/tmp/x509up_u`id -u` -e $EXPERIMENT --directory `pwd`/data --metadata meta.json --name ${dataset}
   #sam_add_dataset --cert=/tmp/x509up_u`id -u` -e $EXPERIMENT `pwd`/data meta.json ${dataset}

   echo "dataset $dataset contains:" 
   ifdh translateConstraints "defname: $dataset" 
}

test_add_dataset_directory() {
   mkdir data
   for i in 1 2 3 
   do
       fname="f${i}.txt"
       echo "file $i" > data/$fname
   done

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

   sam_add_dataset --cert=/tmp/x509up_u`id -u` -e $EXPERIMENT -f file_list --metadata meta.json --name ${dataset}
   #sam_add_dataset --cert=/tmp/x509up_u`id -u` -e $EXPERIMENT  file_list meta.json ${dataset}

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
       echo "file $i" > $fname
       checksum=`ifdh checksum $fname 2>/dev/null | 
			grep '"crc_value"' | 
			sed -e 's/",.*//' -e 's/.*"//'`
       size=`cat $fname | wc -c`
       cat > $fname.json <<EOF
{
 "file_name": "$fname", 
 "file_type": "test", 
 "file_format": "data", 
 "file_size": $size, 
 "checksum": [
  "enstore:$checksum"
 ], 
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
       /pnfs/*) location="enstore:`pwd`";;
       /grid/*) location="${EXPERIMENT}data:`pwd`";;
       /nova/*) location="samdevdata:`pwd`";;
       esac

       samweb declare-file $fname.json
       samweb add-file-location $fname $location
   done
   samweb create-definition $dataset "file_name like '${dataset}_f%'"

   echo "dataset $dataset contains:" 
   ifdh translateConstraints "defname: $dataset" 
}

test_validate_1() {
    sam_validate_dataset --name $dataset
}

test_validate_2() {
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


test_clone() {
    echo "before:"
    sam_validate_dataset -v --name $dataset 2>/dev/null
    locs1=`sam_validate_dataset -v --name  $dataset 2>/dev/null | wc -l`
    echo "----------------------------------"
    sam_clone_dataset -v -b 2 --name $dataset --dest $pnfs_dir
    echo "----------------------------------"
    echo "after:"
    sam_validate_dataset -v --name $dataset 2>/dev/null
    locs2=`sam_validate_dataset -v --name $dataset 2>/dev/null | wc -l`
    [ "$locs2" -gt "$locs1" ]
}

test_clone_n() {
    echo "before:"
    sam_validate_dataset -v --name $dataset 2>/dev/null
    locs1=`sam_validate_dataset -v --name  $dataset 2>/dev/null | wc -l`
    echo "----------------------------------"
    sam_clone_dataset -v -N 3 -b 2 --name $dataset --dest $pnfs_dir
    echo "----------------------------------"
    echo "after:"
    sam_validate_dataset -v --name $dataset 2>/dev/null
    locs2=`sam_validate_dataset -v --name $dataset 2>/dev/null | wc -l`
    [ "$locs2" -gt "$locs1" ]
}

test_copy2scratch_dataset() {
    echo "before:"
    sam_validate_dataset -v --name $dataset
    locs1=`sam_validate_dataset -v --name  $dataset | wc -l`
    sam_copy2scratch_dataset -v --name $dataset --dest $pnfs_dir
    echo "after:"
    sam_validate_dataset -v --name $dataset
    locs2=`sam_validate_dataset -v --name $dataset | wc -l`
    [ "$locs2" -gt "$locs1" ]
}

test_move2archive() {
    echo "before:"
    sam_validate_dataset -v --name $dataset
    locs1=`sam_validate_dataset -v --name  $dataset | wc -l`
    sam_move2archive_dataset -v --name $dataset --dest $pnfs_dir
    echo "after:"
    sam_validate_dataset -v --name $dataset
    locs2=`sam_validate_dataset -v --name $dataset | wc -l`
    [ "$locs2" -gt "$locs1" ]
}

test_move2persistent() {
    echo "before:"
    sam_validate_dataset -v --name $dataset
    locs1=`sam_validate_dataset -v --name  $dataset | wc -l`
    sam_move2persistent_dataset -v --name $dataset --dest $pnfs_dir
    echo "after:"
    sam_validate_dataset -v --name $dataset
    locs2=`sam_validate_dataset -v --name $dataset | wc -l`
    [ "$locs2" -gt "$locs1" ]
}

test_archive_dataset() {
    echo "before:"
    sam_validate_dataset -v --name $dataset
    locs1=`sam_validate_dataset -v --name  $dataset | wc -l`
    # use a non-archving location for testing!
    sam_archive_dataset -v --name $dataset --dest $pnfs_dir
    echo "after:"
    sam_validate_dataset -v --name $dataset
    locs2=`sam_validate_dataset -v --name $dataset | wc -l`
    [ "$locs2" -gt "$locs1" ]
}

test_unclone() {
    echo "before:"
    sam_validate_dataset -v --name $dataset
    locs1=`sam_validate_dataset -v --name $dataset 2>/dev/null | wc -l`
    sam_clone_dataset -v --name $dataset --dest $pnfs_dir
    echo "after:"
    sam_validate_dataset -v --name $dataset
    locs2=`sam_validate_dataset -v --name $dataset 2>/dev/null | wc -l`
    sam_unclone_dataset -v --name $dataset --dest $pnfs_dir/
    echo "after unclone:"
    sam_validate_dataset -v --name $dataset
    locs3=`sam_validate_dataset -v --name $dataset 2>/dev/null | wc -l`
    [ "$locs2" -gt "$locs1" -a "$locs3" -lt "$locs2" ]
}
test_unclone_slashes() {
    # same as the other, but extra slashes in unclone --dest param
    echo "before:"
    sam_validate_dataset -v --name $dataset
    locs1=`sam_validate_dataset -v --name $dataset | wc -l`
    sam_clone_dataset -v --name $dataset --dest $pnfs_dir
    echo "after:"
    sam_validate_dataset -v --name $dataset
    locs2=`sam_validate_dataset -v --name $dataset | wc -l`
    sam_unclone_dataset --name $dataset --dest /pnfs/nova//scratch//users/$USER/fife_util_test/
    echo "after unclone:"
    sam_validate_dataset -v --name $dataset
    locs3=`sam_validate_dataset -v --name $dataset | wc -l`
    [ "$locs2" -gt "$locs1" -a "$locs3" -lt "$locs2" ]
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
    echo "before:"
    sam_validate_dataset -v --name $dataset 2>/dev/null
    locs1=`sam_validate_dataset -v --name  $dataset 2>/dev/null | wc -l`
    echo "----------------------------------"
    sam_clone_dataset -v --project=${proj} --just-start-project -b 2 --name $dataset --dest $pnfs_dir
    echo "----------------------------------"
    sam_clone_dataset -v --project=${proj} --connect-project    -b 2 --name $dataset --dest $pnfs_dir
    echo "----------------------------------"
    echo "after:"
    sam_validate_dataset -v --name $dataset 2>/dev/null
    locs2=`sam_validate_dataset -v --name $dataset 2>/dev/null | wc -l`
    [ "$locs2" -gt "$locs1" ]
}


test_retire() {
    sam_retire_dataset --name $dataset
}

testsuite test_utils \
	-s setup_tests \
        add_dataset \
	test_validate_1 \
	test_validate_2 \
        test_modify \
	test_clone  \
        test_retire \
        add_dataset \
        test_unclone \
        test_unclone_slashes \
        test_pin \
        test_retire \
        add_dataset \
	test_clone_n  \
        test_retire \
        test_add_dataset_flist \
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
         
test_utils "$@"
