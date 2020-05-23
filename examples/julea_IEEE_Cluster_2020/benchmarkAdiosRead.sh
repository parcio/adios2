#! /usr/bin/env bash

#100= more than in any file -> all
# vars="1 4 8 16 32 100"
vars="1 4 8 16 32"
# vars="32"
# files="1 2 4 8 16 32 64 128 256 512"
files="1 4 16 64 256 512"
# files="1 2 4"
# files="1 2 4"
# engines="julea-kv julea-db"
engines="bp3 bp4"
# engines="bp3"

declare -A ending
declare -A result

sum=0
ending=(["bp3"]="bp" ["bp4"]="bp" ["julea-kv"]="jv" ["julea-db"]="jb")

# echo -e "#interlines\t iterations\t io-time[s] ${engines[*]}"
echo -e "#files\t #vars\t io-time[s] ${engines[*]}"
# for interline in $interlines
for fileCount in $files
do
	for varCount in $vars
	# for iteration in $iterations
	do
		for engine in $engines
		do
			outfile=/tmp/AdiosReadBenchmark/$engine-$fileCount-$varCount.${ending[$engine]}

			result[$engine]="$(./bin/BENCHMARK -d testFiles -c $fileCount -p $varCount -n $engine -s2 )\t"
			# result[$engine]="$(./bin/BENCHMARK -d $engine-Files -c $fileCount -p $varCount -n $engine -s2 )\t"
		done
		echo -e "$fileCount\t $varCount\t ${result[*]} \t"
	done
done
