set -ue

function change_name {
    suffix=${1: -4}
    new_name=${1%$suffix}$2".txt"
    mv $new_name $1
}

function build_df {
	echo 'building dataset df'
	tmp=$(mktemp tmp.XXXXXX)
	awk -F, '$5>0{print $0}' data/train.csv >data/train.bk.csv
	mv data/train.bk.csv data/train.csv
	cat data/train.csv >$tmp
	awk 'BEGIN{FS=OFS=","} {$(NF+1)=0} NR>1{tmp=$(NF-1);$(NF-1)=$NF;$NF=tmp;print}' data/test.csv>>$tmp
	awk 'BEGIN{FS=OFS=","} NR==FNR{a[$3,$4];next} ($3,$4) in a' data/test.csv $tmp>data/df.csv
	rm -f $tmp
}

function build_discrete {
	echo 'building discrete data'
	awk -F, '!($4 in seen)&&NR>1{print $4};{seen[$4]}' data/test.csv>temp/uniq_item.txt
	awk 'BEGIN{FS=OFS=","} NR==FNR{$5!~/[1-9]*\.[0-9]?[1-9]+/?a[$4]++:b;
	     c[$4]++;next} $0 in a{if(a[$0]==c[$0]) print $0,1;else print $0,0;next}' data/train.csv temp/uniq_item.txt >data/is_discrete.txt
}

function get_start_date {
    echo 'getting start date for each store_nbr item_nbr'
    awk -F, -v OFS=, '!(($3,$4) in seen)&&NR>1{print $3,$4};{seen[$3,$4]}' data/test.csv>temp/uniq_item_store.txt
    seq 0 1687 | xargs -I {} date -d '2013-1-1 {} days' +%Y-%m-%d>temp/dates.txt
    cp  temp/dates.txt temp/dates_v1.txt
    cat temp/dates.txt | xargs -I {} date -d "{}" +%s | paste -d, temp/uniq_item_store.txt temp/dates_v1.txt ->temp/dates_dict.txt    
    rm temp/dates.txt  temp/dates_v1.txt
    awk -F, -v OFS=, -v max_date=$(date -d "2018-1-1" +%s) 'NR==FNR{a[$1","$2]=max_date;b[$3]=$4;next} b[$2]<a[$3","$4]{key=$3","$4;a[key]=b[$2];
                                   c[key]=$2} END{for(k in c) print k,c[k]}' temp/dates_dict.txt data/train.csv>data/start_date.txt 
}

function find_min_max_date {
    if [ $# -ne 4 ];then
        echo "number of inputs:path to data;column no;[min,max];output path,invalid number of params" >&2
        exit 1
    fi
    
    if [ ! -f $4 ];then
        awk -F, -v OFS=, -v col=$2 '!seen[$col]++' $1 | awk -F, -v col=$2 'NR>1{print $col}' | xargs -I {} date -d {} +%s | sort -n |
                                    xargs -I {} date -d @{} | xargs -I {} date -d {} +%Y-%m-%d>$4
    fi
    
    case $3 in 
        "min")
            ddate=$(cat $4 | head -n1);;
        "max")
            ddate=$(cat $4 | tail -n1);;
        *)
            echo  $3 "invalid option,please pick from [min,max]">&2
            exit 1
    esac
    echo $ddate
}

function build_pivot_data {
    echo "building $2 data"
    delete=""
    for param in "$@";do
        if [ $param = "-d" ]; then delete="-d"; break; fi
    done
    
    max_index=$[ $(cat data/dt_to_idx.txt | wc -l) - 1 ]
    awk -F, -v OFS=, 'NF=2' data/start_date.txt>temp/valid_store_item.txt
    
    split -l 40000 temp/valid_store_item.txt temp/valid_SI
    counter=0
    for file in temp/valid_SI*;do
        df_name="temp/df.split.$counter.txt"
        test -f $df_name || awk -F, -v OFS=, 'NR==FNR{a[$1,$2];next} ($3,$4) in a' $file data/df.csv> $df_name
        awk -F, -v max_num=$max_index -v col=$1 -v nan_value=$3 'BEGIN{r="";n=max_num} NR==FNR{a[$3,$4,$NF]=$col;next} {for(i=0;i<=n;i++) if(($1,$2,i) in a) 
        r=r","a[$1,$2,i];else r=r","nan_value} {r=$1","$2","substr(r,2);print r;r=""}' $df_name $file >temp/trash.$counter.txt
        counter=$[ $counter+1 ]
        [ -n "$delete" ] && rm $file $df_name
    done
    
    output_dir=$2".txt"
    cat temp/trash.*>temp/$output_dir
    tmp=$(mktemp tmp.XXX)
    echo $(seq 0 $max_index) | awk '{$0="store_nbr item_nbr "$0} 1' | awk -v OFS=, '$1=$1'>$tmp
    cat $tmp temp/$output_dir>data/$output_dir
    rm $tmp temp/$output_dir temp/trash.*
}

function encode {
    data=${!#}
    dname=${data:5:-4}
    for col in $@;do
        if [ $col != $data ];then
            echo "encoding column $col of data $dname"
            tmpf=$(mktemp tmpf.XXX)
            col_index=$(head -n1 $data | awk -F, -v col=$col '{for(i=1;i<=NF;i++) if($i==col) print i;exit}')
            awk -F, -v col=$col_index -v n=0 -v OFS=, '!($col in a)&&NR>1{a[$col]=n;n++} END{for(k in a) print k,a[k]}' $data >$tmpf
            if [[ $col =~ ('item'|'store') ]];then
                tmpx=$(mktemp tmpx.XXX)
                tmpz=$(mktemp tmpz.XXX)
                awk -F, -v col=$col_index 'NR>1{print $col}' $data > $tmpx
                awk -F, -v col=$col_index 'NR==FNR{a[$1]=$2;next} FNR>1{print a[$col]}' $tmpf $data > $tmpz
                paste -d, $tmpx $tmpz > temp/item_store.txt
                rm $tmpx $tmpf $tmpz
                continue
            fi
            backup_file=${data%${data: -4}}".bk.txt"
            awk -F, -v OFS=, -v col=$col_index 'NR==FNR{a[$1]=$2;next} FNR>1{$col=a[$col];print;next} {print $0}' $tmpf $data > $backup_file
            rm $tmpf
            change_name $data .bk
        fi
    done
}

function single_col_concat {
    dname=${5:5:-4}
    echo "merge column $4 of data $dname"
    tmp_name=${6%${6: -3}}"bk.txt"
    awk -v inp_key=$1 -v var=$2 -v col_name=$4 -v out_key=$3 'BEGIN{FS=OFS=","} NR==FNR{a[$inp_key]=$var;next} 
                                                              {$(NF+1)=FNR==1?col_name:a[$out_key]} 1' $5 $6 > $tmp_name
    change_name $6 .bk
}

mkdir -p temp
build_df
cat data/df.csv | awk -F, -v OFS=, '$NF~/^$/{$NF=2} 1' | sed 's/True/1/g' | sed 's/False/0/g'>data/df.bk.csv
mv data/df.bk.csv data/df.csv

build_discrete
get_start_date
rm -f data/train.csv data/test.csv

df_min_date=$(find_min_max_date data/df.csv 2 min temp/df_dates.txt)
df_max_date=$(find_min_max_date data/df.csv 2 max temp/df_dates.txt | xargs -I {} date -d {} +%s)
min_date_insecs=$(echo $df_min_date | xargs -I {} date -d {} +%s)
diff=$((df_max_date-min_date_insecs))
diff_days=$[$((diff/86400))]
seq 0 $diff_days >data/dt_to_idx.bk.txt
cat data/dt_to_idx.bk.txt | xargs -I {} date -d "$df_min_date {} days" +%Y-%m-%d | paste -d, - data/dt_to_idx.bk.txt>data/dt_to_idx.txt
rm data/dt_to_idx.bk.txt
awk -F, -v OFS=, 'NR==FNR{a[$1]=$2;next} {$(NF+1)=FNR==1?"date_idx":a[$2]} 1' data/dt_to_idx.txt data/df.csv>data/df.bk.csv
mv data/df.bk.csv data/df.csv

build_pivot_data 6 op 2
build_pivot_data 5 x_raw 0
build_pivot_data 1 id 0 -d

awk -F, -v OFS=, 'NR==FNR{a[$1]=$2;next} ($3 in a){$3=a[$3];print;next} {print $3  "is not a valid date";exit}' data/dt_to_idx.txt data/start_date.txt >data/start_date.bk.txt
change_name data/start_date.txt .bk
awk -F, -v OFS=, 'NR==FNR{a[$1,$2]=$3;next} {$(NF+1)=FNR==1?"start_date":a[$1,$2];print}' data/start_date.txt data/x_raw.txt >data/x_raw.bk.txt
change_name data/x_raw.txt .bk
rm data/start_date.txt data/dt_to_idx.txt

stores_cols=("city"
             "state"
             "type"
             "cluster")
items_cols=("family"
            "class"
            "item_nbr")
            
awk -v FPAT='"[^"]*"|[^,]*' -v OFS=' ' '{gsub(/ /,"");$1=$1;print}' data/items.csv | awk -v OFS=, '{gsub(/,/,"");$1=$1} 1' > data/items.txt
mv data/items.txt data/items.csv
encode ${stores_cols[*]} data/stores.csv
encode ${items_cols[*]} data/items.csv

num=1
for ((i=0;i<${#stores_cols[@]};i++));do
    num=$[ $num+1 ]
    file=${stores_cols[i]}
    single_col_concat 1 $num 1 $file data/stores.csv data/x_raw.txt
done

num=1
concat_item_cols=("${items_cols[*]:0:${#items_cols[@]}-1}")
for item_col in $concat_item_cols;do
    num=$[ $num+1 ]
    single_col_concat 1 $num 2 $item_col data/items.csv data/x_raw.txt
done

single_col_concat 1 2 2 'is_discrete' data/is_discrete.txt data/x_raw.txt
single_col_concat 1 4 2 'perishable' data/items.csv data/x_raw.txt
single_col_concat 1 2 2 'item_enc' temp/item_store.txt data/x_raw.txt

features=(
    'store_nbr'
    'item_enc'
    'city'
    'state'
    'type'
    'cluster'
    'family'
    'class'
    'perishable'
    'is_discrete'
    'start_date'
)

for feature in ${features[*]};do
    print_col=$(head -n1 data/x_raw.txt | awk -F, -v feat=$feature '{for(i=1;i<=NF;i++) if($i==feat) print i;exit}')
    awk -F, -v print_col=$print_col '{print $print_col}' data/x_raw.txt > "data/$feature.txt"
done

python shell_scripts/get_lag.py
find data -name '*.csv' -exec rm {} \;