set -e

taxi_type=$1 #'yellow'
year=$2 #2019

# 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_2019-01.csv.gz'

url_prefix='https://github.com/DataTalksClub/nyc-tlc-data/releases/download/'

for month in {1..12}; do

    month=`printf '%02d' ${month}`
    url="${url_prefix}/${taxi_type}/${taxi_type}_tripdata_${year}-${month}.csv.gz"
    
    local_path="taxi_data/${taxi_type}/${year}/${month}"
    filename="${taxi_type}_tripdata_${year}-${month}.csv.gz"

    mkdir -p "${local_path}"

    echo "Downloading data files:"
    wget ${url} -O "${local_path}/${filename}"
    #gunzip "${local_path}/${filename}"

    echo "${filename} succesfully stored in ${local_path}"

done
