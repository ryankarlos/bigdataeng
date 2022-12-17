while IFS="," read -r rec
if [ ! -z "$rec" ]
then
  do
    wget -c 2> /dev/null;
fi;
done < <(cut -d "\t" -f2 inputfile)
