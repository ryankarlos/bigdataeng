#!/bin/bash

inputfile=$1
func=${2:-simplified}


function complex()
{
for a in `yes | nl | head -50 | cut -f 1`; do \
  head -$(($a*2)) inputfile | tail -1 | \
  awk 'BEGIN{FS="\t"}{print $2}' | xargs wget -c 2> /dev/null;
done
}


function simplified()
{
  while read -r line ;
  do
   if [ ! -z "${line}" ];
   then
      wget -c ${line} 2> /dev/null;
   fi;
  done < <(cut -f2 inputfile | tail -n +2)

}

$func