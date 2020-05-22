#! /usr/bin/env bash
#engines="bp3 bp4 julea-kv julea-db"
engines="julea-db"

declare -A ending
declare -A result

ending=(["bp3"]="bp" ["bp4"]="bp" ["julea-kv"]="jv" ["julea-db"]="jb")

# echo -e "#interlines\t iterations\t io-time[s] ${engines[*]}"
echo -e "engine\t io-time[ms] \t file"

for file in /home/duwe/ieee_cluster_2020_adios2/ecmwf-data/*.nc
do
	for engine in $engines
	do
		#outfile=/tmp/$engine-Files/$file.${ending[$engine]}
		filename=$(basename -- "$file")
		extension="${filename##*.}"
		filename="${filename%.*}"
		outfile=/tmp/$engine-Files/$filename.${ending[$engine]}
		#echo -e "$filename"
		path=julea-adios2/build/bin/NC_TO_BP
		# 	read fileCount often varCount many vars from engine-Files
		result[$engine]="$(./$path -d $file -f $outfile -n $engine -t )\t"
	done
	echo -e "$engine\t  ${result[*]} \t $filename"
	# echo -e "$interline\t $iteration\t ${result[*]}"
done

