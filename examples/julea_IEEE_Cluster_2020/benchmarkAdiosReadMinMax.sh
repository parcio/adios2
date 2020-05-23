#! /usr/bin/env bash

#100= more than in any file -> all
# vars="1 4 8 16 32 100"
vars="1 4 8 16 32"
# files="1 2 4 8 16 32 64 128 256 512"
files="1 4 16 64 256 512"

# engines="julea-kv julea-db"
engines="bp3"
maximum="42 420 4200"

declare -A ending
declare -A result

ending=(["bp3"]="bp" ["bp4"]="bp" ["julea-kv"]="jv" ["julea-db"]="jb")

# echo -e "number of Files\t number of Variables\t value to compare against\t io-time[s]\t number of blocks that had larger maximum\t - used engines: ${engines[*]} "
echo -e "#files (number of Files\t) #vars (number of Variables)\t largerThan (value to compare against)\t io-time[s] (time to find all matching blocks) \t #wereLarger ( number of blocks that had larger maximum)\t"
echo -e "#files\t #vars\t largerThan\t io-time[s]\t #wereLarger\t - used engines: ${engines[*]} "
for fileCount in $files
do
	for varCount in $vars
	do
		for max in $maximum
		do
			for engine in $engines
			do
				outfile=/tmp/AdiosReadBenchmark/$engine-$fileCount-$varCount.${ending[$engine]}

				result[$engine]="$(./bin/BENCHMARK -d testFiles -c $fileCount -p $varCount -n $engine -s3 -m $max )\t"
				# result[$engine]="$(./bin/BENCHMARK -d /tmp/$engine-Files -c $fileCount -p $varCount -n $engine -s3 -m 2000 )\t"
			done
		echo -e "$fileCount\t $varCount\t $max\t ${result[*]} \t"
		done
	done
done
