#!/bin/sh


echo "================================================"
echo "fake_exe: $*"
 
printenv

doloop=false
for a in "$@" 
do
   case x$a in
   x--sam-web-uri=*)
        cpurl="${a#--sam-web-uri=}"
        doloop=true
	;;
   x--sam-process-id=*)
        consumer_id="${a#--sam-process-id=}"
        doloop=true
	;;
   esac
done

if $doloop
then 

    echo "Doing fake ifdh_art file loop"
    furi=`ifdh getNextFile $cpurl $consumer_id`
    while [ "$furi"  != "" ]
    do
	fname=`ifdh fetchInput $furi | tail -1 `
        echo "in fake_exe:  handling file $furi as $fname"
	ifdh updateFileStatus $cpurl  $consumer_id $fname transferred
	sleep 1
	ifdh updateFileStatus $cpurl  $consumer_id $fname consumed
        rm -f $fname
        furi=`ifdh getNextFile $cpurl $consumer_id`
    done
fi

echo "================================================"

exit 0
