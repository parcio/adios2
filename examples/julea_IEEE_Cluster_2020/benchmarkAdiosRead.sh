#! /usr/bin/env bash

#100= more than in any file -> all
vars="1 4 8 16 32 100"
files="1 2 4 8 16 32 64 128 256 512"
engines="bp3 bp4 julea-kv julea-db"

declare -A ending
declare -A result

ending=(["bp3"]="bp" ["bp4"]="bp" ["julea-kv"]="jv" ["julea-db"]="jb")

# echo -e "#interlines\t iterations\t io-time[s] ${engines[*]}"
echo -e "#files\t vars\t io-time[s] ${engines[*]}"
# for interline in $interlines
for fileCount in $files
do
	for varCount in $vars
	# for iteration in $iterations
	do
		for engine in $engines
		do
			outfile=/tmp/AdiosReadBenchmark/$engine-$fileCount-$varCount.${ending[$engine]}
			#outfile=$engine-$interline-$iteration.${ending[$engine]}
			# 100 nach $iteration für checkpoints in jeder
			# iteration (100%)
			# result[$engine]="$(./partdiff-par \
			# 	1 2 $interline 1 2 $iteration 100\
			# 	$outfile $engine | grep I/O | tr -d ' ' | \
			# 	tr -d 's' | cut -d : -f 2)\t"
			# 	read fileCount often varCount many vars from engine-Files
			result[$engine]="$(./bin/BENCHMARK -d $engine-Files -c $fileCount -p $varCount -n $engine -s2 )\t"
		done
		echo -e "$fileCount\t $varCount\t ${result[*]}"
		# echo -e "$interline\t $iteration\t ${result[*]}"
	done
done
