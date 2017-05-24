if [ $(ps -ef | grep 'hyperlocal_v2.py'| wc -l) -lt 2 ]
then
  (python /home/ubuntu/yadu/graphite_dashboard/hyperlocal_v2.py >> /home/ubuntu/yadu/graphite_dashboard/hyperlocal_v2.log 2>&1 &)
fi
