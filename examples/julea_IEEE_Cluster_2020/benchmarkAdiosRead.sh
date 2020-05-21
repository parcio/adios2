#! /usr/bin/env bash

iterations="1 2 4 8"
#interlines="0 64 512 1024 2048 4096"
interlines="4096"
engines="bp3 bp4 julea-kv julea-db"
declare -A ending 
declare -A result
ending=(["bp3"]="bp" ["bp4"]="bp" ["julea-kv"]="jv" ["julea-db"]="jb")

echo -e "#interlines\t iterations\t io-time[s] ${engines[*]}"
for interline in $interlines
do
	for iteration in $iterations
	do
		for engine in $engines
		do
			outfile=/tmp/bpFiles/$engine-$interline-$iteration.${ending[$engine]}
			#outfile=$engine-$interline-$iteration.${ending[$engine]}
			# 100 nach $iteration für checkpoints in jeder
			# iteration (100%)
			result[$engine]="$(./partdiff-par \
				1 2 $interline 1 2 $iteration 100\
				$outfile $engine | grep I/O | tr -d ' ' | \
				tr -d 's' | cut -d : -f 2)\t"
		done
		echo -e "$interline\t $iteration\t ${result[*]}"
	done
done
