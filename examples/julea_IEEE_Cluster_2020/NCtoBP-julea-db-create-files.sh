#! /usr/bin/env bash
#engines="bp3 bp4 julea-kv julea-db"
engines="julea-db"

declare -A ending
declare -A result

ending=(["bp3"]="bp" ["bp4"]="bp" ["julea-kv"]="jv" ["julea-db"]="jb")

# echo -e "#interlines\t iterations\t io-time[s] ${engines[*]}"
echo -e "engine\t io-time[ms] \t file"

for engine in $engines
do
mkdir /tmp/$engine-Files
	for file in /home/duwe/ieee_cluster_2020_adios2/ecmwf-data/*.nc
	do
		filename=$(basename -- "$file")
		filename="${filename%.*}"
		outfile=/tmp/$engine-Files/$filename.${ending[$engine]}
		touch "$outfile"
	done
done

