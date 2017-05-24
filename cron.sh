if [ $(ps -ef | grep 'hyperlocal_v3.py'| wc -l) -lt 2 ]
then
  (python /home/ubuntu/anuran/kibana/hyperlocal_v3.py >> /home/ubuntu/anuran/kibana/hyperlocal_v3.log 2>&1 &)
fi
if [ $(ps -ef | grep 'rider_location.py'| wc -l) -lt 2 ]
then
  (python /home/ubuntu/anuran/kibana/rider_location.py >> /home/ubuntu/anuran/kibana/rider_location.log 2>&1 &)
fi
if [ $(ps -ef | grep 'delivery_service.py'| wc -l) -lt 2 ]
then
  (python /home/ubuntu/anuran/kibana/delivery_service.py >> /home/ubuntu/anuran/kibana/delivery_service.log 2>&1 &)
fi
if [ $(ps -ef | grep 'rider_attendance.py'| wc -l) -lt 2 ]
then
  (python /home/ubuntu/anuran/kibana/rider_attendance.py >> /home/ubuntu/anuran/kibana/rider_attendance.log 2>&1 &)
fi
if [ $(ps -ef | grep 'otp_data.py'| wc -l) -lt 2 ]
then
  (python /home/ubuntu/anuran/kibana/otp_data.py >> /home/ubuntu/anuran/kibana/otp_data.log 2>&1 &)
fi
