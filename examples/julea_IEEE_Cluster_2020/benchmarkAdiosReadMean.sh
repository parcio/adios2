#! /usr/bin/env bash

varNames="t2m stl1"
files="1 4 16 64 256 512"

# engines="julea-kv julea-db"
engines="bp3"

declare -A ending
declare -A result

ending=(["bp3"]="bp" ["bp4"]="bp" ["julea-kv"]="jv" ["julea-db"]="jb")

echo -e "#files\t varName\t io-time[s]\t - used engines: ${engines[*]} "
for fileCount in $files
do
    for varName in $varNames
    do
        for engine in $engines
        do
        outfile=/tmp/AdiosReadBenchmark/$engine-$fileCount-$varCount.${ending[$engine]}
        # result[$engine]="$(./bin/BENCHMARK -d testFiles -c $fileCount -n $engine -s 4 -k $varName )\t"
        result[$engine]="$(./bin/BENCHMARK -d /tmp/$engine-Files -c $fileCount -n $engine -s 4 -k $varName )\t"
        done
        echo -e "$fileCount\t $varName\t ${result[*]} \t"
    done
done
