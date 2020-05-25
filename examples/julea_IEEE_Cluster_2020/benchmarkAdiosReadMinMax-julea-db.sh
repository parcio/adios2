#! /usr/bin/env bash

#32 = all; 27 variables are in nc files
vars="1 4 8 16 26"
# vars="1 4 8 16"
# files="1 2 4 8 16 32 64 128 256 512"
# files="1 4 16 64 256 512"
# files="1 4 16 64"
files="250"

# engines="julea-kv julea-db"
engines="julea-db"
#engines="bp3 bp4"
maximum="42"
# maximum="42 420 4200"

declare -A ending
declare -A result

ending=(["bp3"]="bp" ["bp4"]="bp" ["julea-kv"]="jv" ["julea-db"]="jb")

# echo -e "number of Files\t number of Variables\t value to compare against\t io-time[s]\t number of blocks that had larger maximum\t - used engines: ${engines[*]} "
echo -e "#files (number of Files\t) #vars (number of Variables)\t largerThan (value to compare against)\t io-time[s] (time to find all matching blocks) \t #wereLarger ( number of blocks that had larger maximum)\t"
echo -e "#files\t #vars\t largerThan\t io-time[ms]\t #wereLarger\t - used engines: ${engines[*]} "
for fileCount in $files
do
	for varCount in $vars
	do
		for max in $maximum
		do
			for engine in $engines
			do
				outfile=/tmp/AdiosReadBenchmark/$engine-$fileCount-$varCount.${ending[$engine]}

				# result[$engine]="$(./bin/BENCHMARK -d testFiles -c $fileCount -p $varCount -n $engine -s3 -m $max )\t"
				result[$engine]="$(./julea-adios2/build/bin/BENCHMARK -d /tmp/$engine-Files -c $fileCount -p $varCount -n $engine -s3 -m $max )\t"
			done
		echo -e "$fileCount\t $varCount\t $max\t ${result[*]} \t"
		done
	done
done
