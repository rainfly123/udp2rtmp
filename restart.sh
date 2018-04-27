PID=$(ps ax|grep udp2rtmpd|grep -v grep|awk '{print $1}')
echo $PID
kill -9 $PID
/xiechc/udp2rtmpd -f /xiechc/channels.cfg -p 1234
