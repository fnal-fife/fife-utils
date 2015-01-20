#!/bin/bash

. unittest.bash

setup_tests() {
   export EXPERIMENT=samdev
   export SAM_EXPERIMENT=samdev
   workdir=/tmp/work.$$
   if [ ! -r $workdir ]
   then
       mkdir $workdir
   fi
   if [ ! -r $workdir/datset ] 
   then
       echo "testds_`hostname -fqdn`_`date +%s`" > /tmp/ds.$$
   fi
   cd $workdir
   read dataset < dataset
   export dataset
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
       checksum=`ifdh checksum $fname`
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
 }, 
}
EOF
       case `pwd` in
       /pnfs/*) location="enstore:`pwd`";;
       /grid/*) location="${EXPERIMENT}data:`pwd`";;
       /nova/*) location="novadata:`pwd`";;
       esac

       samweb declare-file $fname.json
       samweb add-file-location $file $location
   done
   samweb create-definition $dataset "file-name like '${dataset}_f%'"
}

test_validate() {
    sam_validate_dataset $dataset
}

test_clone() {
    sam_clone_dataset $dataset /scratch/nova/users/$USER
    sam_validate_dataset $dataset
}

testsuite test_utils \
	-s setup_tests \
	test_validate \
	test_clone 
