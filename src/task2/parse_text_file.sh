#!/bin/bash

inputfile=$1

function complex()
{
for a in `yes | nl | head -50 | cut -f 1`; do \
  head -$(($a*2)) inputfile | tail -1 | \
  awk 'BEGIN{FS="\t"}{print $2}' | xargs wget -c ;
done
}


function simplified()
{
  awk 'NR%2==0{print $2}' inputfile | head -50 | \
  while read -r line ; \
  do
      wget -c ${line} 2> /dev/null;
  done
}

simplified