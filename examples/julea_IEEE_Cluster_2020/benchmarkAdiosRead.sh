#! /usr/bin/env bash

vars="1 4 8 16 32"

# engines="julea-kv julea-db"
engines="bp3 bp4"
# engines="bp3"

filenumber=0

declare -A ending
declare -A result

ending=(["bp3"]="bp" ["bp4"]="bp" ["julea-kv"]="jv" ["julea-db"]="jb")

echo -e "#files\t #vars\t io-time[s] ${engines[*]}"

# for file in /home/duwe/ieee_cluster_2020_adios2/ecmwf-data/*.nc
for file in /tmp/$engine-Files
do
	let "filenumber +=1"
	for varCount in $vars
	do
		for engine in $engines
		do
			filename=$(basename -- "$file")
			extension="${filename##*.}"
			filename="${filename%.*}"
			#outfile=/tmp/AdiosReadBenchmark/$engine-$fileCount-$varCount.${ending[$engine]}

			result[$engine]="$(./bin/BENCHMARK -d file -c 1 -p $varCount -n $engine -s2 )\t"
			# result[$engine]="$(./bin/BENCHMARK -d /tmp/$engine-Files -c $fileCount -p $varCount -n $engine -s2 )\t"
		done
	echo -e "$filenumber\t $varCount\t ${result[*]} filename\t"
	done
done

