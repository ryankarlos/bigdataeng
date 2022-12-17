#!/bin/bash

for a in `yes | nl | head -50 | cut -f 1`; do \
  head -$(($a*2)) inputfile | tail -1 | \
  awk 'BEGIN{FS="\t"}{print $2}' | xargs wget -c 2> /dev/null;
done