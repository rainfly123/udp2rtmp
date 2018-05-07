/*
The MIT License (MIT)

Copyright (c) 2013-2015 SRS(ossrs)

Permission is hereby granted, free of charge, to any person obtaining a copy of
this software and associated documentation files (the "Software"), to deal in
the Software without restriction, including without limitation the rights to
use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of
the Software, and to permit persons to whom the Software is furnished to do so,
subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS
FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER
IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
*/
#include <jemalloc/jemalloc.h>
#include <srs_core.hpp>
#include <stdlib.h>
#include <string>
#include <vector>
#include <map>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/time.h>
#include <sys/types.h>
#include <unistd.h>

using namespace std;

#include <srs_kernel_error.hpp>
#include <srs_app_server.hpp>
#include <srs_app_config.hpp>
#include <srs_app_log.hpp>
#include <srs_kernel_utility.hpp>
#include <srs_rtmp_stack.hpp>
#include <srs_kernel_buffer.hpp>
#include <srs_kernel_stream.hpp>
#include <srs_kernel_ts.hpp>
#include <srs_app_http_client.hpp>
#include <srs_core_autofree.hpp>
#include <srs_app_st.hpp>
#include <srs_rtmp_utility.hpp>
#include <srs_app_st.hpp>
#include <srs_app_utility.hpp>
#include <srs_rtmp_amf0.hpp>
#include <srs_raw_avc.hpp>
#include <srs_app_http_conn.hpp>
#include <pthread.h>  
#include <iostream>  
#include <fstream>  
#include <string.h>  
#include <srs_app_thread.hpp>
#include <srs_app_listener.hpp>

using std::cout;  
using std::endl;  
const int QUEUESIZE = 1024;  
template<class Object>  
class ThreadQueue  
{  
public:  
    ThreadQueue();  
    ~ThreadQueue();  
public:  
    bool Enter(Object *obj);  
    Object* Out();  
    bool IsEmpty();  
    bool IsFull();  
private:  
    int front;  
    int rear;  
    int size;  
    Object *list[QUEUESIZE];  
    pthread_mutex_t queueMutex;  
    pthread_cond_t queueCondx;  
};  
template<class Object>
ThreadQueue<Object>::ThreadQueue()
{  
    front = rear = 0;
    size = QUEUESIZE;
    pthread_cond_init(&queueCondx, NULL);
    pthread_mutex_init(&queueMutex, NULL);
} 
template<class Object> 
bool ThreadQueue<Object>::Enter(Object* obj)
{
    pthread_mutex_lock(&queueMutex);
    if(IsFull())
    {
        cout << "Queue is full!" << endl; 
        pthread_mutex_unlock(&queueMutex);
  
        return false;
    }
    list[rear] = obj;
    rear = (rear + 1) % size;
    pthread_cond_signal(&queueCondx); 
    pthread_mutex_unlock(&queueMutex);
  
    return true;
}
template<class Object>
Object* ThreadQueue<Object>::Out()
{
    Object* temp;
    struct timespec abstime;
    struct timeval now;

    pthread_mutex_lock(&queueMutex);
    if (IsEmpty()) {
        gettimeofday(&now, NULL);
        abstime.tv_sec = now.tv_sec;
        abstime.tv_nsec = now.tv_usec * 1000;
        pthread_cond_timedwait(&queueCondx, &queueMutex, &abstime); 
        if (IsEmpty()) {
            //time out
            pthread_mutex_unlock(&queueMutex);
            return NULL;
         }
    }
    temp = list[front];
    front = (front + 1) % size;
  
    pthread_mutex_unlock(&queueMutex);
  
    return temp;
}
template<class Object>
bool ThreadQueue<Object>::IsEmpty()
{
    if(rear == front)
        return true;
    else
        return false;
}
template<class Object>
bool ThreadQueue<Object>::IsFull()
{
    if((rear + 1) % size == front)
        return true;
    else
        return false;
} 
template<class Object>
ThreadQueue<Object>::~ThreadQueue()
{
}  

struct Data  
{  
    char buf[1328];  
    uint16_t size;
    uint16_t refer;
};  
  
// the context to ingest hls stream.
class SrsIngestSrsInput
{
public:
    ThreadQueue <Data> * mbuffer;
private:
    SrsHttpUri* in_hls;
private:
    SrsStream* stream;
    SrsTsContext* context;
public:
    SrsIngestSrsInput(int16_t program_number) {
        
        stream = new SrsStream();
        context = new SrsTsContext();
        context->program_number = program_number;
        mbuffer  = new ThreadQueue<Data>();
    }
    virtual ~SrsIngestSrsInput() {
        srs_freep(stream);
        srs_freep(context);
        srs_freep(mbuffer);
    }
    /**
     * parse the ts and use hanler to process the message.
     */
    virtual int parse(ISrsTsHandler* ts);
private:
    /**
     * parse the ts pieces body.
     */
    virtual int parseTs(ISrsTsHandler* handler, char* body, int nb_body);
};

int SrsIngestSrsInput::parse(ISrsTsHandler* ts)
{
    int ret = ERROR_SUCCESS;
    Data *temp;  
    while ((temp = mbuffer->Out()) != NULL) {
        ret = parseTs(ts, temp->buf, temp->size);
        temp->refer -= 1;
        if (temp->refer == 0)
           delete temp;
    }
    
    return ret;
}

int SrsIngestSrsInput::parseTs(ISrsTsHandler* handler, char* body, int nb_body)
{
    int ret = ERROR_SUCCESS;
    
    // use stream to parse ts packet.
    int nb_packet =  (int)nb_body / SRS_TS_PACKET_SIZE;
    for (int i = 0; i < nb_packet; i++) {
        char* p = (char*)body + (i * SRS_TS_PACKET_SIZE);
        if ((ret = stream->initialize(p, SRS_TS_PACKET_SIZE)) != ERROR_SUCCESS) {
            return ret;
        }
        
        // process each ts packet
        if ((ret = context->decode(stream, handler)) != ERROR_SUCCESS) {
            srs_error("mpegts: ignore parse ts packet failed. ret=%d", ret);
            return ret;
        }
    }
    srs_info("mpegts: parse udp packet completed");
    
    return ret;
}

// the context to output to rtmp server
class SrsIngestSrsOutput : virtual public ISrsTsHandler 
{
private:
    SrsHttpUri* out_rtmp;
private:
    bool disconnected;
    std::multimap<int64_t, SrsTsMessage*> queue;
    int64_t raw_aac_dts;
private:
    SrsRequest* req;
    st_netfd_t stfd;
    SrsStSocket* io;
    SrsRtmpClient* client;
    int stream_id;
private:
    SrsRawH264Stream* avc;
    std::string h264_sps;
    bool h264_sps_changed;
    std::string h264_pps;
    bool h264_pps_changed;
    bool h264_sps_pps_sent;
private:
    SrsRawAacStream* aac;
    std::string aac_specific_config;
public:
    SrsIngestSrsOutput(SrsHttpUri* rtmp) {
        out_rtmp = rtmp;
        disconnected = false;
        raw_aac_dts = srs_update_system_time_ms();
        
        req = NULL;
        io = NULL;
        client = NULL;
        stfd = NULL;
        stream_id = 0;
        
        avc = new SrsRawH264Stream();
        aac = new SrsRawAacStream();
        h264_sps_changed = false;
        h264_pps_changed = false;
        h264_sps_pps_sent = false;
    }
    virtual ~SrsIngestSrsOutput() {
        close();
        
        srs_freep(avc);
        srs_freep(aac);
        
        std::multimap<int64_t, SrsTsMessage*>::iterator it;
        for (it = queue.begin(); it != queue.end(); ++it) {
            SrsTsMessage* msg = it->second;
            srs_freep(msg);
        }
        queue.clear();
    }
// interface ISrsTsHandler
public:
    virtual int on_ts_message(SrsTsMessage* msg);
private:
    virtual int parse_message_queue();
    virtual int on_ts_video(SrsTsMessage* msg, SrsStream* avs);
    virtual int write_h264_sps_pps(u_int32_t dts, u_int32_t pts);
    virtual int write_h264_ipb_frame(std::string ibps, SrsCodecVideoAVCFrame frame_type, u_int32_t dts, u_int32_t pts);
    virtual int on_ts_audio(SrsTsMessage* msg, SrsStream* avs);
    virtual int write_audio_raw_frame(char* frame, int frame_size, SrsRawAacStreamCodec* codec, u_int32_t dts);
private:
    virtual int rtmp_write_packet(char type, u_int32_t timestamp, char* data, int size);
public:
    /**
     * connect to output rtmp server.
     */
    virtual int connect();
    /**
     * flush the message queue when all ts parsed.
     */
    virtual int flush_message_queue();

    virtual void close();

private:
    virtual int connect_app(std::string ep_server, std::string ep_port);
    // close the connected io and rtmp to ready to be re-connect.
};

int SrsIngestSrsOutput::on_ts_message(SrsTsMessage* msg)
{
    int ret = ERROR_SUCCESS;
    
    // about the bytes of msg, specified by elementary stream which indicates by PES_packet_data_byte and stream_id
    // for example, when SrsTsStream of SrsTsChannel indicates stream_type is SrsTsStreamVideoMpeg4 and SrsTsStreamAudioMpeg4,
    // the elementary stream can be mux in "2.11 Carriage of ISO/IEC 14496 data" in hls-mpeg-ts-iso13818-1.pdf, page 103
    // @remark, the most popular stream_id is 0xe0 for h.264 over mpegts, which indicates the stream_id is video and
    //      stream_number is 0, where I guess the elementary is specified in annexb format(H.264-AVC-ISO_IEC_14496-10.pdf, page 211).
    //      because when audio stream_number is 0, the elementary is ADTS(aac-mp4a-format-ISO_IEC_14496-3+2001.pdf, page 75, 1.A.2.2 ADTS).
    
    // about the bytes of PES_packet_data_byte, defined in hls-mpeg-ts-iso13818-1.pdf, page 58
    // PES_packet_data_byte ¨C PES_packet_data_bytes shall be contiguous bytes of data from the elementary stream
    // indicated by the packet¡¯s stream_id or PID. When the elementary stream data conforms to ITU-T
    // Rec. H.262 | ISO/IEC 13818-2 or ISO/IEC 13818-3, the PES_packet_data_bytes shall be byte aligned to the bytes of this
    // Recommendation | International Standard. The byte-order of the elementary stream shall be preserved. The number of
    // PES_packet_data_bytes, N, is specified by the PES_packet_length field. N shall be equal to the value indicated in the
    // PES_packet_length minus the number of bytes between the last byte of the PES_packet_length field and the first
    // PES_packet_data_byte.
    //
    // In the case of a private_stream_1, private_stream_2, ECM_stream, or EMM_stream, the contents of the
    // PES_packet_data_byte field are user definable and will not be specified by ITU-T | ISO/IEC in the future.
    
    // about the bytes of stream_id, define in  hls-mpeg-ts-iso13818-1.pdf, page 49
    // stream_id ¨C In Program Streams, the stream_id specifies the type and number of the elementary stream as defined by the
    // stream_id Table 2-18. In Transport Streams, the stream_id may be set to any valid value which correctly describes the
    // elementary stream type as defined in Table 2-18. In Transport Streams, the elementary stream type is specified in the
    // Program Specific Information as specified in 2.4.4.
    
    // about the stream_id table, define in Table 2-18 ¨C Stream_id assignments, hls-mpeg-ts-iso13818-1.pdf, page 52.
    //
    // 110x xxxx
    // ISO/IEC 13818-3 or ISO/IEC 11172-3 or ISO/IEC 13818-7 or ISO/IEC
    // 14496-3 audio stream number x xxxx
    // ((sid >> 5) & 0x07) == SrsTsPESStreamIdAudio
    //
    // 1110 xxxx
    // ITU-T Rec. H.262 | ISO/IEC 13818-2 or ISO/IEC 11172-2 or ISO/IEC
    // 14496-2 video stream number xxxx
    // ((stream_id >> 4) & 0x0f) == SrsTsPESStreamIdVideo
    
    // When the audio SID is private stream 1, we use common audio.
    // @see https://github.com/ossrs/srs/issues/740
    if (msg->channel->apply == SrsTsPidApplyAudio && msg->sid == SrsTsPESStreamIdPrivateStream1) {
        msg->sid = SrsTsPESStreamIdAudioCommon;
    }
    
    // when not audio/video, or not adts/annexb format, donot support.
    if (msg->stream_number() != 0) {
        ret = ERROR_STREAM_CASTER_TS_ES;
        srs_error("mpegts: unsupported stream format, sid=%#x(%s-%d). ret=%d",
                  msg->sid, msg->is_audio()? "A":msg->is_video()? "V":"N", msg->stream_number(), ret);
        return ret;
    }
    
    // check supported codec
    if (msg->channel->stream != SrsTsStreamVideoH264 && msg->channel->stream != SrsTsStreamAudioAAC) {
        ret = ERROR_STREAM_CASTER_TS_CODEC;
        srs_error("mpegts: unsupported stream codec=%d. ret=%d", msg->channel->stream, ret);
        return ret;
    }
    
    // we must use queue to cache the msg, then parse it if possible.
    queue.insert(std::make_pair(msg->dts, msg->detach()));
    if ((ret = parse_message_queue()) != ERROR_SUCCESS) {
        return ret;
    }
    
    return ret;
}

int SrsIngestSrsOutput::parse_message_queue()
{
    int ret = ERROR_SUCCESS;
    
    if (queue.empty()) {
        return ret;
    }
    
    SrsTsMessage* first_ts_msg = queue.begin()->second;
    SrsTsContext* context = first_ts_msg->channel->context;
    bool cpa = context->is_pure_audio();
    
    int nb_videos = 0;
    if (!cpa) {
        std::multimap<int64_t, SrsTsMessage*>::iterator it;
        for (it = queue.begin(); it != queue.end(); ++it) {
            SrsTsMessage* msg = it->second;
            
            // publish audio or video.
            if (msg->channel->stream == SrsTsStreamVideoH264) {
                nb_videos++;
            }
        }
        
        // always wait 2+ videos, to left one video in the queue.
        // TODO: FIXME: support pure audio hls.
        if (nb_videos <= 1) {
            return ret;
        }
    }
    
    // parse messages util the last video.
    while ((cpa && queue.size() > 1) || nb_videos > 1) {
        srs_assert(!queue.empty());
        std::multimap<int64_t, SrsTsMessage*>::iterator it = queue.begin();
        
        SrsTsMessage* msg = it->second;
        if (msg->channel->stream == SrsTsStreamVideoH264) {
            nb_videos--;
        }
        queue.erase(it);
        
        // parse the stream.
        SrsStream avs;
        if ((ret = avs.initialize(msg->payload->bytes(), msg->payload->length())) != ERROR_SUCCESS) {
            srs_error("mpegts: initialize av stream failed. ret=%d", ret);
            srs_freep(msg); //xiechangcai add
            return ret;
        }
        
        // publish audio or video.
        if (msg->channel->stream == SrsTsStreamVideoH264) {
            if ((ret = on_ts_video(msg, &avs)) != ERROR_SUCCESS) {
                srs_freep(msg); //xiechangcai add
                return ret;
            }
        }
        else if (msg->channel->stream == SrsTsStreamAudioAAC) {
            if ((ret = on_ts_audio(msg, &avs)) != ERROR_SUCCESS) {
                srs_freep(msg); //xiechangcai add
                return ret;
            }
        }
       srs_freep(msg); //xiechangcai add
    }
    
    return ret;
}

int SrsIngestSrsOutput::flush_message_queue()
{
    int ret = ERROR_SUCCESS;
    
    // parse messages util the last video.
    while (!queue.empty()) {
        std::multimap<int64_t, SrsTsMessage*>::iterator it = queue.begin();
        
        SrsTsMessage* msg = it->second;
        queue.erase(it);
        
        // parse the stream.
        SrsStream avs;
        if ((ret = avs.initialize(msg->payload->bytes(), msg->payload->length())) != ERROR_SUCCESS) {
            srs_error("mpegts: initialize av stream failed. ret=%d", ret);
            srs_freep(msg); //xiechangcai add
            return ret;
        }
        
        // publish audio or video.
        if (msg->channel->stream == SrsTsStreamVideoH264) {
            if ((ret = on_ts_video(msg, &avs)) != ERROR_SUCCESS) {
                srs_freep(msg); //xiechangcai add
                return ret;
            }
        }
        if (msg->channel->stream == SrsTsStreamAudioAAC) {
            if ((ret = on_ts_audio(msg, &avs)) != ERROR_SUCCESS) {
                srs_freep(msg); //xiechangcai add
                return ret;
            }
        }
        srs_freep(msg); //xiechangcai add
    }
    
    return ret;
}

int SrsIngestSrsOutput::on_ts_video(SrsTsMessage* msg, SrsStream* avs)
{
    int ret = ERROR_SUCCESS;
    
    // ts tbn to flv tbn.
    u_int32_t dts = (u_int32_t)(msg->dts / 90);
    u_int32_t pts = (u_int32_t)(msg->pts / 90);
    
    std::string ibps;
    SrsCodecVideoAVCFrame frame_type = SrsCodecVideoAVCFrameInterFrame;
    
    // send each frame.
    while (!avs->empty()) {
        char* frame = NULL;
        int frame_size = 0;
        if ((ret = avc->annexb_demux(avs, &frame, &frame_size)) != ERROR_SUCCESS) {
            return ret;
        }
        
        // 5bits, 7.3.1 NAL unit syntax,
        // H.264-AVC-ISO_IEC_14496-10.pdf, page 44.
        //  7: SPS, 8: PPS, 5: I Frame, 1: P Frame
        SrsAvcNaluType nal_unit_type = (SrsAvcNaluType)(frame[0] & 0x1f);
        
        // for IDR frame, the frame is keyframe.
        if (nal_unit_type == SrsAvcNaluTypeIDR) {
            frame_type = SrsCodecVideoAVCFrameKeyFrame;
        }
        
        // ignore the nalu type aud(9)
        if (nal_unit_type == SrsAvcNaluTypeAccessUnitDelimiter) {
            continue;
        }
        
        // for sps
        if (avc->is_sps(frame, frame_size)) {
            std::string sps;
            if ((ret = avc->sps_demux(frame, frame_size, sps)) != ERROR_SUCCESS) {
                return ret;
            }
            
            if (h264_sps == sps) {
                continue;
            }
            h264_sps_changed = true;
            h264_sps = sps;
            continue;
        }
        
        // for pps
        if (avc->is_pps(frame, frame_size)) {
            std::string pps;
            if ((ret = avc->pps_demux(frame, frame_size, pps)) != ERROR_SUCCESS) {
                return ret;
            }
            
            if (h264_pps == pps) {
                continue;
            }
            h264_pps_changed = true;
            h264_pps = pps;
            continue;
        }
        
        // ibp frame.
        std::string ibp;
        if ((ret = avc->mux_ipb_frame(frame, frame_size, ibp)) != ERROR_SUCCESS) {
            return ret;
        }
        ibps.append(ibp);
    }
    
    if ((ret = write_h264_sps_pps(dts, pts)) != ERROR_SUCCESS) {
        return ret;
    }
    
    if ((ret = write_h264_ipb_frame(ibps, frame_type, dts, pts)) != ERROR_SUCCESS) {
        // drop the ts message.
        if (ret == ERROR_H264_DROP_BEFORE_SPS_PPS) {
            return ERROR_SUCCESS;
        }
        return ret;
    }
    
    return ret;
}

int SrsIngestSrsOutput::write_h264_sps_pps(u_int32_t dts, u_int32_t pts)
{
    int ret = ERROR_SUCCESS;
    
    // when sps or pps changed, update the sequence header,
    // for the pps maybe not changed while sps changed.
    // so, we must check when each video ts message frame parsed.
    if (h264_sps_pps_sent && !h264_sps_changed && !h264_pps_changed) {
        return ret;
    }
    
    // when not got sps/pps, wait.
    if (h264_pps.empty() || h264_sps.empty()) {
        return ret;
    }
    
    // h264 raw to h264 packet.
    std::string sh;
    if ((ret = avc->mux_sequence_header(h264_sps, h264_pps, dts, pts, sh)) != ERROR_SUCCESS) {
        return ret;
    }
    
    // h264 packet to flv packet.
    int8_t frame_type = SrsCodecVideoAVCFrameKeyFrame;
    int8_t avc_packet_type = SrsCodecVideoAVCTypeSequenceHeader;
    char* flv = NULL;
    int nb_flv = 0;
    if ((ret = avc->mux_avc2flv(sh, frame_type, avc_packet_type, dts, pts, &flv, &nb_flv)) != ERROR_SUCCESS) {
        return ret;
    }
    
    // the timestamp in rtmp message header is dts.
    u_int32_t timestamp = dts;
    if ((ret = rtmp_write_packet(SrsCodecFlvTagVideo, timestamp, flv, nb_flv)) != ERROR_SUCCESS) {
        return ret;
    }
    
    // reset sps and pps.
    h264_sps_changed = false;
    h264_pps_changed = false;
    h264_sps_pps_sent = true;
    srs_trace("hls: h264 sps/pps sent, sps=%dB, pps=%dB", h264_sps.length(), h264_pps.length());
    
    return ret;
}

int SrsIngestSrsOutput::write_h264_ipb_frame(string ibps, SrsCodecVideoAVCFrame frame_type, u_int32_t dts, u_int32_t pts)
{
    int ret = ERROR_SUCCESS;
    
    // when sps or pps not sent, ignore the packet.
    // @see https://github.com/ossrs/srs/issues/203
    if (!h264_sps_pps_sent) {
        return ERROR_H264_DROP_BEFORE_SPS_PPS;
    }
    
    int8_t avc_packet_type = SrsCodecVideoAVCTypeNALU;
    char* flv = NULL;
    int nb_flv = 0;
    if ((ret = avc->mux_avc2flv(ibps, frame_type, avc_packet_type, dts, pts, &flv, &nb_flv)) != ERROR_SUCCESS) {
        return ret;
    }
    
    // the timestamp in rtmp message header is dts.
    u_int32_t timestamp = dts;
    return rtmp_write_packet(SrsCodecFlvTagVideo, timestamp, flv, nb_flv);
}

int SrsIngestSrsOutput::on_ts_audio(SrsTsMessage* msg, SrsStream* avs)
{
    int ret = ERROR_SUCCESS;
    
    // ts tbn to flv tbn.
    u_int32_t dts = (u_int32_t)(msg->dts / 90);
    
    // got the next msg to calc the delta duration for each audio.
    u_int32_t duration = 0;
    if (!queue.empty()) {
        SrsTsMessage* nm = queue.begin()->second;
        duration = (u_int32_t)(srs_max(0, nm->dts - msg->dts) / 90);
    }
    u_int32_t max_dts = dts + duration;
    
    // send each frame.
    while (!avs->empty()) {
        char* frame = NULL;
        int frame_size = 0;
        SrsRawAacStreamCodec codec;
        if ((ret = aac->adts_demux(avs, &frame, &frame_size, codec)) != ERROR_SUCCESS) {
            return ret;
        }
        
        // ignore invalid frame,
        //  * atleast 1bytes for aac to decode the data.
        if (frame_size <= 0) {
            continue;
        }
        srs_info("mpegts: demux aac frame size=%d, dts=%d", frame_size, dts);
        
        // generate sh.
        if (aac_specific_config.empty()) {
            std::string sh;
            if ((ret = aac->mux_sequence_header(&codec, sh)) != ERROR_SUCCESS) {
                return ret;
            }
            aac_specific_config = sh;
            
            codec.aac_packet_type = 0;
            
            if ((ret = write_audio_raw_frame((char*)sh.data(), (int)sh.length(), &codec, dts)) != ERROR_SUCCESS) {
                return ret;
            }
        }
        
        // audio raw data.
        codec.aac_packet_type = 1;
        if ((ret = write_audio_raw_frame(frame, frame_size, &codec, dts)) != ERROR_SUCCESS) {
            return ret;
        }
        
        // calc the delta of dts, when previous frame output.
        u_int32_t delta = duration / (msg->payload->length() / frame_size);
        dts = (u_int32_t)(srs_min(max_dts, dts + delta));
    }
    
    return ret;
}

int SrsIngestSrsOutput::write_audio_raw_frame(char* frame, int frame_size, SrsRawAacStreamCodec* codec, u_int32_t dts)
{
    int ret = ERROR_SUCCESS;
    
    char* data = NULL;
    int size = 0;
    if ((ret = aac->mux_aac2flv(frame, frame_size, codec, dts, &data, &size)) != ERROR_SUCCESS) {
        return ret;
    }
    
    return rtmp_write_packet(SrsCodecFlvTagAudio, dts, data, size);
}

int SrsIngestSrsOutput::rtmp_write_packet(char type, u_int32_t timestamp, char* data, int size)
{
    int ret = ERROR_SUCCESS;
    
    SrsSharedPtrMessage* msg = NULL;
    
    if ((ret = srs_rtmp_create_msg(type, timestamp, data, size, stream_id, &msg)) != ERROR_SUCCESS) {
        srs_error("mpegts: create shared ptr msg failed. ret=%d", ret);
        return ret;
    }
    srs_assert(msg);
    
    srs_info("RTMP type=%d, dts=%d, size=%d", type, timestamp, size);
    
    // send out encoded msg.
    if ((ret = client->send_and_free_message(msg, stream_id)) != ERROR_SUCCESS) {
        srs_error("send RTMP type=%d, dts=%d, size=%d failed. ret=%d", type, timestamp, size, ret);
        return ret;
    }
    
    return ret;
}

int SrsIngestSrsOutput::connect()
{
    int ret = ERROR_SUCCESS;
    
    // when ok, ignore.
    // TODO: FIXME: should reconnect when disconnected.
    if (io || client) {
        return ret;
    }
    
    srs_trace("connect output=%s", out_rtmp->get_url());
    
    // parse uri
    if (!req) {
        req = new SrsRequest();
        
        string uri = req->tcUrl = out_rtmp->get_url();
        
        // tcUrl, stream
        if (srs_string_contains(uri, "/")) {
            req->stream = srs_path_basename(uri);
            req->tcUrl = uri = srs_path_dirname(uri);
        }
        
        srs_discovery_tc_url(req->tcUrl,
            req->schema, req->host, req->vhost, req->app, req->port,
            req->param);
    }
    
    // connect host.
    if ((ret = srs_socket_connect(req->host, ::atoi(req->port.c_str()), ST_UTIME_NO_TIMEOUT, &stfd)) != ERROR_SUCCESS) {
        srs_error("mpegts: connect server %s:%s failed. ret=%d", req->host.c_str(), req->port.c_str(), ret);
        return ret;
    }
    io = new SrsStSocket(stfd);
    client = new SrsRtmpClient(io);
    
    client->set_recv_timeout(SRS_CONSTS_RTMP_RECV_TIMEOUT_US);
    client->set_send_timeout(SRS_CONSTS_RTMP_SEND_TIMEOUT_US);
    
    // connect to vhost/app
    if ((ret = client->handshake()) != ERROR_SUCCESS) {
        srs_error("mpegts: handshake with server failed. ret=%d", ret);
        return ret;
    }
    if ((ret = connect_app(req->host, req->port)) != ERROR_SUCCESS) {
        srs_error("mpegts: connect with server failed. ret=%d", ret);
        return ret;
    }
    if ((ret = client->create_stream(stream_id)) != ERROR_SUCCESS) {
        srs_error("mpegts: connect with server failed, stream_id=%d. ret=%d", stream_id, ret);
        return ret;
    }
    
    // publish.
    if ((ret = client->publish(req->stream, stream_id)) != ERROR_SUCCESS) {
        srs_error("mpegts: publish failed, stream=%s, stream_id=%d. ret=%d",
                  req->stream.c_str(), stream_id, ret);
        return ret;
    }
    
    return ret;
}

// TODO: FIXME: refine the connect_app.
int SrsIngestSrsOutput::connect_app(string ep_server, string ep_port)
{
    int ret = ERROR_SUCCESS;
    
    // args of request takes the srs info.
    if (req->args == NULL) {
        req->args = SrsAmf0Any::object();
    }
    
    // notify server the edge identity,
    // @see https://github.com/ossrs/srs/issues/147
    SrsAmf0Object* data = req->args;
    data->set("srs_sig", SrsAmf0Any::str(RTMP_SIG_SRS_KEY));
    data->set("srs_server", SrsAmf0Any::str(RTMP_SIG_SRS_KEY" "RTMP_SIG_SRS_VERSION" ("RTMP_SIG_SRS_URL_SHORT")"));
    data->set("srs_license", SrsAmf0Any::str(RTMP_SIG_SRS_LICENSE));
    data->set("srs_role", SrsAmf0Any::str(RTMP_SIG_SRS_ROLE));
    data->set("srs_url", SrsAmf0Any::str(RTMP_SIG_SRS_URL));
    data->set("srs_version", SrsAmf0Any::str(RTMP_SIG_SRS_VERSION));
    data->set("srs_site", SrsAmf0Any::str(RTMP_SIG_SRS_WEB));
    data->set("srs_email", SrsAmf0Any::str(RTMP_SIG_SRS_EMAIL));
    data->set("srs_copyright", SrsAmf0Any::str(RTMP_SIG_SRS_COPYRIGHT));
    data->set("srs_primary", SrsAmf0Any::str(RTMP_SIG_SRS_PRIMARY));
    data->set("srs_authors", SrsAmf0Any::str(RTMP_SIG_SRS_AUTHROS));
    // for edge to directly get the id of client.
    data->set("srs_pid", SrsAmf0Any::number(getpid()));
    data->set("srs_id", SrsAmf0Any::number(_srs_context->get_id()));
    
    // local ip of edge
    std::vector<std::string> ips = srs_get_local_ipv4_ips();
    assert(0 < (int)ips.size());
    std::string local_ip = ips[0];
    data->set("srs_server_ip", SrsAmf0Any::str(local_ip.c_str()));
    
    // generate the tcUrl
    std::string param = "";
    std::string tc_url = srs_generate_tc_url(ep_server, req->vhost, req->app, ep_port, param);
    
    // upnode server identity will show in the connect_app of client.
    // @see https://github.com/ossrs/srs/issues/160
    // the debug_srs_upnode is config in vhost and default to true.
    bool debug_srs_upnode = true;
    if ((ret = client->connect_app(req->app, tc_url, req, debug_srs_upnode)) != ERROR_SUCCESS) {
        srs_error("mpegts: connect with server failed, tcUrl=%s, dsu=%d. ret=%d",
                  tc_url.c_str(), debug_srs_upnode, ret);
        return ret;
    }
    
    return ret;
}

void SrsIngestSrsOutput::close()
{
    srs_trace("close output=%s", out_rtmp->get_url());
    h264_sps_pps_sent = false;
    aac_specific_config.clear();
    srs_freep(client);
    srs_freep(io);
    srs_freep(req);
    srs_close_stfd(stfd);
}

// the context for ingest hls stream.
class SrsIngestSrsContext
{
public:
    SrsIngestSrsInput* ic;
    SrsIngestSrsOutput* oc;
    SrsIngestSrsContext(SrsHttpUri* rtmp, int16_t program_number) {
        ic = new SrsIngestSrsInput(program_number);
        oc = new SrsIngestSrsOutput(rtmp);
    }
    virtual ~SrsIngestSrsContext() {
        srs_freep(ic);
        srs_freep(oc);
    }
    virtual int proxy() {
        int ret = ERROR_SUCCESS;
        
        if ((ret = oc->connect()) != ERROR_SUCCESS) {
            srs_error("connect ic failed. ret=%d", ret);
            //clear the memory 
            Data *temp;  
            while ((temp = ic->mbuffer->Out()) != NULL) {
                temp->refer -= 1;
                if (temp->refer == 0)
                   delete temp;
            }
            return ret;
        }
        
        if ((ret = ic->parse(oc)) != ERROR_SUCCESS) {
            srs_error("proxy ts to rtmp failed. ret=%d", ret);
            oc->close();
            return ret;
        }
        
        if ((ret = oc->flush_message_queue()) != ERROR_SUCCESS) {
            srs_error("flush oc message failed. ret=%d", ret);
            oc->close();
            return ret;
        }
        
        return ret;
    }
};
class udpRecv
{
public:
    udpRecv();
    virtual ~udpRecv();
    int listen(uint16_t port);
    int skt;
    void Recv();
    static void* thread_fun(void* arg);
    void startRecv();
    void addHandler(SrsIngestSrsContext * p);
private:
    SrsIngestSrsContext* all[50];
    int number;
};

udpRecv::udpRecv(){
    number = 0;
    skt= socket(AF_INET, SOCK_DGRAM, IPPROTO_UDP );
}

udpRecv::~udpRecv(){
}

void udpRecv::addHandler(SrsIngestSrsContext * p){
 all[number++] = p;
}

int udpRecv::listen(uint16_t port){
    struct sockaddr_in local;
    memset( &local, 0, sizeof(local) );
    local.sin_family = AF_INET;
    local.sin_addr.s_addr = inet_addr("0.0.0.0");
    local.sin_port = htons(port);
    return bind(skt, (struct sockaddr *)&local,    sizeof(local)) ;
}

void* udpRecv::thread_fun(void* arg)
{
   udpRecv *p = (udpRecv *) arg;
   while (1)
   p->Recv();
}

void udpRecv::startRecv(){
   pthread_t tid;
   pthread_create(&tid, NULL, thread_fun, this);
   pthread_detach(tid);
}

void udpRecv::Recv(){
    uint32_t slen ;

    char buf[1532];
    size_t size = sizeof(buf);

    fd_set rfds;
    struct timeval tv;
    int retval;
    bool val;
    tv.tv_sec = 0;
    tv.tv_usec = 50 * 1000;

    while (1) {
    FD_ZERO(&rfds);
    FD_SET(skt, &rfds);
    retval = select(skt + 1, &rfds, NULL, NULL, &tv);
    if (retval <= 0)
       usleep(10000);
    else if (retval) {
        slen = recvfrom(skt, buf, size, 0, NULL, 0);
        if (slen > 0) {
            Data * piece = new Data();  
            piece->size = slen;
            piece->refer = number;
            memcpy(piece->buf, buf, slen);

            for (int i = 0;i < number; i++) {
               if ((val = all[i]->ic->mbuffer->Enter(piece)) != true) {
                   piece->refer -= 1;
               }
            }
        }
    }
   }

}

SrsIngestSrsContext * proxy_hls2rtmp(string rtmp, int16_t program_number,  udpRecv *recv)
{
    int ret = ERROR_SUCCESS;
    
    
    SrsHttpUri * rtmp_uri = new SrsHttpUri();
    if ((ret = rtmp_uri->initialize(rtmp)) != ERROR_SUCCESS) {
        srs_error("rtmp uri invalid. ret=%d", ret);
        return NULL;
    }
    
    SrsIngestSrsContext * context = new SrsIngestSrsContext (rtmp_uri, program_number);
    recv->addHandler(context);
    return context;
}

ISrsLog* _srs_log = new SrsFastLog();
ISrsThreadContext* _srs_context = new SrsThreadContext();
// app module.
SrsConfig* _srs_config = NULL;
SrsServer* _srs_server = NULL;

int main(int argc, char** argv) 
{
    // TODO: support both little and big endian.
    //daemon(1,0);
    srs_assert(srs_is_little_endian());
    
    // directly failed when compile limited.
#if !defined(SRS_AUTO_HTTP_CORE)
    srs_error("depends on http-parser.");
    exit(-1);
#endif
    
#if defined(SRS_AUTO_GPERF_MP) || defined(SRS_AUTO_GPERF_MP) \
|| defined(SRS_AUTO_GPERF_MC) || defined(SRS_AUTO_GPERF_MP)
    srs_error("donot support gmc/gmp/gcp/gprof");
    exit(-1);
#endif
    
    // parse user options.
    std::string out_rtmp_url;
    char * config_file;
    uint16_t listen_port ;
    for (int opt = 0; opt < argc; opt++) {
        srs_trace("argv[%d]=%s", opt, argv[opt]);
    }
    
    // fill the options for mac
    for (int opt = 0; opt < argc - 1; opt++) {
        // ignore all options except -i and -y.
        char* p = argv[opt];
        // only accept -x
        if (p[0] != '-' || p[1] == 0 || p[2] != 0) {
            continue;
        }
        // parse according the option name.
        switch (p[1]) {
            case 'f': config_file = argv[opt + 1]; break;
            case 'p': listen_port = atoi(argv[opt + 1]); break;
            default: break;
        }
    }
    
   if (argc != 5 ) {
        printf("ingest udp ts stream and publish to RTMP server\n"
               "Usage: %s <-f config file path>  <-p udp port>\n"
               "   config file path    read configs from config file path\n"
               "   udp port            listen udp at this port.\n"
               "For example:\n"
               "   %s -f ./channels.cfg -p 1234\n",
               argv[0], argv[0]);
        exit(-1);
    }
   
     struct Channels {
      string rtmp;
      uint16_t program_number;
     };
     Channels all[50];
     //memset(all, 0, sizeof(all));
     ifstream fin (config_file, ios::in);  
     string s;  
     int which = 0;
     while ( getline(fin,s) )
     {    
          if (s.length() < 1) continue;
          std::size_t found;
          found = s.find("#");
          if (found != std::string::npos) continue;
          string program_number;
          string rtmp_url;
          found = s.find(" ");
          program_number  = s.substr(0, found);
          rtmp_url  = s.substr(found + 1);
          cout << " program_number: " << program_number << endl; 
          cout << " url: " << rtmp_url << endl; 
          all[which].rtmp = rtmp_url; 
          all[which].program_number = ::atoi(program_number.c_str()); 
          which += 1;
     } 
    
    udpRecv *recv = new udpRecv();
    recv->listen(listen_port);
    SrsIngestSrsContext * channels[50];
    int c = 0;
    for (; c < which; c++)
        channels[c] = NULL;

    // init st.
    int ret;
    if ((ret = srs_st_init()) != ERROR_SUCCESS) {
        srs_error("init st failed");
    }
    for (c = 0; c < which; c++) {
       channels[c] = proxy_hls2rtmp(all[c].rtmp, all[c].program_number, recv);
    }

    recv->startRecv();
    while (1) {
    for (c = 0; c < which; c++) {
        if (channels[c] != NULL)
          channels[c]->proxy() ;
    }
   }
}

