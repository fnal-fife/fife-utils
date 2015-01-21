#!/bin/bash

. unittest.bash

setup_tests() {
   export EXPERIMENT=samdev
   export SAM_EXPERIMENT=samdev
   export IFDH_BASE_URI="http://samweb.fnal.gov:8480/sam/samdev/api"

   workdir=/grid/data/mengel/work.$$
   if [ ! -r $workdir ]
   then
       mkdir $workdir
   fi
   cd $workdir
   if [ ! -r dataset ] 
   then
       echo "testds_`hostname --fqdn`_`date +%s`_$$" > dataset
   fi
   read dataset < dataset
   export dataset
   kx509
   voms-proxy-init -rfc -noregen -debug -voms fermilab:/fermilab/nova/Role=Analysis
   export IFDH_NO_PROXY=1
}

#
# XXX later this should use the end-user command, we're just
# doing an approximation here...
#
add_dataset() {
   for i in 1 2 3 
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
       /nova/*) location="novadata:`pwd`";;
       esac

       samweb declare-file $fname.json
       samweb add-file-location $fname $location
   done
   samweb create-definition $dataset "file_name like '${dataset}_f%'"

   echo "dataset $dataset contains:" 
   ifdh translateConstraints "defname: $dataset" 
}

test_validate_1() {
    sam_validate_dataset $dataset
}

test_validate_2() {
    mv ${dataset}_f2 ${dataset}_f2_hide
    if sam_validate_dataset $dataset
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
    sam_validate_dataset -v $dataset
    locs1=`sam_validate_dataset -v $dataset | wc -l`
    ifdh mkdir /pnfs/nova/scratch/users/$USER/fife_util_test || true
    sam_clone_dataset $dataset /pnfs/nova/scratch/users/$USER/fife_util_test
    echo "after:"
    sam_validate_dataset -v $dataset
    locs2=`sam_validate_dataset -v $dataset | wc -l`
    [ "$locs2" -gt "$locs1" ]
}

test_unclone() {
    echo "before:"
    sam_validate_dataset -v $dataset
    locs1=`sam_validate_dataset -v $dataset | wc -l`
    ifdh mkdir /pnfs/nova/scratch/users/$USER/fife_util_test2 || true
    sam_clone_dataset $dataset /pnfs/nova/scratch/users/$USER/fife_util_test2
    echo "after:"
    sam_validate_dataset -v $dataset
    locs2=`sam_validate_dataset -v $dataset | wc -l`
    sam_unclone_dataset $dataset /pnfs/nova/scratch/users/$USER/fife_util_test
    echo "after unclone:"
    sam_validate_dataset -v $dataset
    locs3=`sam_validate_dataset -v $dataset | wc -l`
    [ "$locs2" -gt "$locs1" -a "$locs3" -lt "$locs2" ]
}

test_pin() {
    sam_pin_dataset $dataset
}

test_retire() {
    sam_retire_dataset $dataset
}

testsuite test_utils \
	-s setup_tests \
        add_dataset \
	test_validate_1 \
	test_validate_2 \
	test_clone  \
        test_unclone \
        test_pin \
        test_retire

test_utils
