set -e

if [ ! -f favorita-grocery-sales-forecasting.zip  ];then
  echo 'downloading data from kaggle'
  kaggle competitions download -c favorita-grocery-sales-forecasting
else
  echo 'grocery data has been downloaded'
fi

mkdir -p data
no_files=$(ls data | wc -l)

if [ $no_files -gt 0 ];then
  echo 'data has already been unzipped'
else
  unzip favorita-grocery-sales-forecasting.zip -d data
fi

find data -iname "*.7z" | while read filename;do
  7z e -y $filename -odata
  rm $filename
done
rm favorita-grocery-sales-forecasting.zip

ls -a data | xargs echo