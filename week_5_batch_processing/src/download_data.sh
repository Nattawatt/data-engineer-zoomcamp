
set -e 

TAXI_TYPE=$1 # yellow
YEAR=$2 # 2020

URL_PREFIX="https://d37ci6vzurychx.cloudfront.net/trip-data"

for MONTH in {1..12}; do
    FMONTH=`printf "%02d" ${MONTH}`

    URL="${URL_PREFIX}/${TAXI_TYPE}_tripdata_${YEAR}-${FMONTH}.parquet"
    
    LOCAL_PREFIX="data/raw/${TAXI_TYPE}/${YEAR}/${FMONTH}"
    LOCAL_FILE="${TAXI_TYPE}_tripdata_${YEAR}_${FMONTH}.parquet"
    LOCAL_PATH="${LOCAL_PREFIX}/${LOCAL_FILE}"
    
    echo "downloading ${URL} to ${LOCAL_PATH}"
    mkdir -p ${LOCAL_PREFIX}
    wget ${URL} -O ${LOCAL_PATH}

    # echo "compressing ${LOCAL_PATH}"
    # gzip ${LOCAL_PATH}
done

