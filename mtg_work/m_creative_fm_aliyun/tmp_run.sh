source ~/.bashrc

start_day=20200809
end_day=20200812
# end_day=$start_day
cur_day=$start_day
while [ $cur_day -le $end_day ]
do
  start_hour="${cur_day}00"
  end_hour="${cur_day}23"
  cd ./kudu_data_for_fm
    sh -x tmp_kudu_data_for_fm.sh $start_hour $end_hour
  cd ..

  cd ./fm_train_data
    sh -x tmp_run_creative_fm.sh $end_hour $end_hour
  cd ../

  cur_day=`date -d "+1 day $cur_day" +%Y%m%d`
done

