# udp2rtmp
Used to receive MPTS(Multiple Program Transport Stream) from  UDP , demux it , 
and then publish SPTS(Single Program Transport Stream) to RTMP server, providing RTMP viewing method.

this codes are from SRS (Simple-Rtmp-Server) [Take a look](https://github.com/ossrs/srs)
and I changed about 500 lines , fixed some bugs .
If you want to use it ,just overwrite the same files in SRS and make SRS
the executable file is located in objs/srs_ingest_hls  (maybe :) )

# NOTICE
files outside the directory "final"
is using Python wrap , it will start several Process(each program using one Process ,what a shame)

files inside the directory "final"
just start one process, (all of programs using the same Process)

# About config file
#program_number rtmp_url     don't care this is comment

1010 rtmp://127.0.0.1/cctv1/live   
1010 is program number in PAT tble (ISO13818-1) ,the following rtmp URL is publish address


if you want to  publish more than one RTMP stream using the same program in MPEG-TS:
just write one more config item in config file with the same program_number
For example:
1020 rtmp://127.0.0.1/cctv2/live
1020 rtmp://127.0.0.1/cctv3/live

you can find me through Wechat.
any amount of donation is appreciated !


![wx](https://github.com/rainfly123/udp2rtmp/blob/master/wx.jpg){:height="50%" width="50%"}
