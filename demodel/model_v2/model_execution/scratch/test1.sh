#!/bin/bash
yesterday=$(date -d "-1 days" +%Y-%m-%d);
cur_path=$(pwd)
logfile=$cur_path/download/Deact_model_organised/Cleaning_modeling_and_predicting/logfile_$yesterday.txt

echo "CV segments: Begin execusion for $yesterday on $(date) \n\n" > $logfile
echo "Step#1: orders query $(date) \n" >> $logfile

python $cur_path/download/Deact_model_2_code.py >> $logfile 2>> $logfile \
   || mailx -s "failure on step 1 | $yesterday | $(date)" ebates@groupon.com < $logfile

mailx -s "CV segments done! | $yesterday | $(date)" ebates@groupon.com < $logfile

