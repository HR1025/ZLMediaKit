/*
 * Copyright (c) 2016-present The ZLMediaKit project authors. All Rights Reserved.
 *
 * This file is part of ZLMediaKit(https://github.com/ZLMediaKit/ZLMediaKit).
 *
 * Use of this source code is governed by MIT-like license that can be found in the
 * LICENSE file in the root of the source tree. All contributing project authors
 * may be found in the AUTHORS file in the root of the source tree.
 */

#include <cassert>
#include <signal.h>
#include <mutex>
#include <deque>
#include <memory>
#include <string>
#include <thread>
#include <chrono>
#include <cstdint>
#include <fstream>
#include <atomic>
#include <iostream>
#include <condition_variable>

#include "mov-format.h"
#include "mov-reader.h"
#include "ext-codec/AAC.h"
#include "ext-codec/H264.h"

#include "Util/logger.h"
#include "Common/config.h"
#include "Common/Parser.h"
#include "Extension/Frame.h"
#include "Poller/EventPoller.h"
#include "Pusher/MediaPusher.h"
#include "Record/MP4Reader.h"
#include "Extension/Factory.h"

using namespace std;
using namespace toolkit;
using namespace mediakit;


///////////////////////////////////////////////////////////////////////////////////////////////////////////////////

//
// 3 thread : 1 reader therad + 2 media timer
// 通过 dts 及 FIFO 实现简易解复用流控
//
class SimpleFrame : public FrameImp
{
public:
    using Ptr = std::shared_ptr<SimpleFrame>;
public:
    bool keyFrame() const override
    {
        return _isKey;
    }
public:
    bool _isKey = false;
};

class SimpleMp4Reader final
{
public:
    using Ptr = std::shared_ptr<SimpleMp4Reader>;
public:
    SimpleMp4Reader(const std::string& vhost, const std::string& app, const std::string& stream_id, const std::string& filePath);
    ~SimpleMp4Reader();
public:
    void Start();
    void Stop();
private: /* override mov_buffer_t function */
	int read(void* param, void* data, uint64_t bytes);
	int write(void* param, const void* data, uint64_t bytes);
	int seek(void* param, uint64_t offset);
	int64_t tell(void* param);
private: /* override mov_reader_trackinfo_t funciton */
    void OnVideo(void* param, uint32_t track, uint8_t object, int width, int height, const void* extra, size_t bytes);
    void OnAudio(void* param, uint32_t track, uint8_t object, int channel_count, int bit_per_sample, int sample_rate, const void* extra, size_t bytes);
private: /* override mov util funcion */
    void* OnMovReaderOnread2(void* param, uint32_t track, size_t bytes, int64_t pts, int64_t dts, int flags);
private:
    std::string _vhost;
    std::string _app;
    std::string _streamId;
    std::string _filePath;
private:
    std::atomic<bool> _running;
    uint32_t _curTrack;
    Frame::Ptr _curFrame;
private:
    std::mutex _sinkMtx;
    MediaSink::Ptr _sink; // hint : thread unsafe
private:
    std::shared_ptr<std::thread>  _demuxerThread;
    std::ifstream _mp4Ifs;
    size_t _fileSize;
private:
    std::mutex _videoMtx;
    std::condition_variable _videoCond;
    uint64_t _videoCacheSize;
    std::deque<Frame::Ptr> _videoCache;
    std::shared_ptr<std::thread> _videoTimer;
    std::atomic<uint32_t> _videoTrack;
private:
    std::mutex _audioMtx;
    std::condition_variable _audioCond;
    uint64_t _audioCacheSize;
    std::deque<Frame::Ptr> _audioCache;
    std::shared_ptr<std::thread> _audioTimer;
    std::atomic<uint32_t> _audioTrack;
};

void* SimpleMp4Reader::OnMovReaderOnread2(void* param, uint32_t track, size_t bytes, int64_t pts, int64_t dts, int flags)
{
    SimpleFrame::Ptr frame = std::make_shared<SimpleFrame>();
    constexpr uint32_t h264PreSize = 4;
    frame->_dts = dts;
    frame->_pts = pts;
    frame->setIndex(track);
    if (track == _videoTrack)
    {
        frame->_buffer.resize(bytes);
        frame->_prefix_size = h264PreSize;
        frame->_codec_id = CodecId::CodecH264;
        if (flags & MOV_AV_FLAG_KEYFREAME)
        {
            frame->_isKey = true;
        }
    }
    else if (track == _audioTrack)
    {
        frame->_buffer.resize(bytes);
        frame->_prefix_size = 0;
        frame->_codec_id = CodecId::CodecAAC;
    }
    _curTrack = track;
    _curFrame = frame;
    return frame->_buffer.data();
}

int SimpleMp4Reader::read(void* /* param */, void* data, uint64_t bytes)
{
    if ((uint64_t)_mp4Ifs.tellg() + bytes > _fileSize)
    {
        return -1;
    }
    _mp4Ifs.read((char*)data, bytes);
    if (bytes != (uint64_t)_mp4Ifs.gcount())
    {
        WarnL << "Read size is: " << _mp4Ifs.gcount() << ", require is: " << bytes << ", Cur is: " << _mp4Ifs.tellg();
    }
    if (_mp4Ifs.eof())
    {
        assert(false);
    }
    return 0;
}

int SimpleMp4Reader::write(void* /* param */, const void* data, uint64_t bytes)
{
    // no need
    return 0;
}

int SimpleMp4Reader::seek(void* /* param */, uint64_t offset)
{
    if (offset <= _fileSize)
    {
        _mp4Ifs.seekg(offset, std::ios::beg);
        return 0;
    }
    else
    {
        return -1;
    }
    
}

int64_t SimpleMp4Reader::tell(void* /* param */)
{
    return (int64_t)_mp4Ifs.tellg();
}

void SimpleMp4Reader::OnVideo(void* /* param */, uint32_t track, uint8_t object, int /* width */, int /* height */, const void* extra, size_t bytes)
{
    Track::Ptr trace;
    switch (object) 
    {
        case MOV_OBJECT_H264:
        {
            trace = Factory::getTrackByCodecId(CodecId::CodecH264);
            if (extra && bytes)
            {
                trace->setExtraData((uint8_t*)extra, bytes);
            }
            break;
        }
        // add if needed
        // case MOV_OBJECT_HEVC:
        // {
        //     ...
        //     break;
        // }
        default:
            break;
    }
    trace->setIndex(track);
    if (trace/* && _sink */)
    {
        std::lock_guard<std::mutex> sinklock(_sinkMtx);
        _sink->addTrack(trace);
    }
    _videoTrack = track;
    InfoL << "Add video track, index is: " << track;
}

void SimpleMp4Reader::OnAudio(void* param, uint32_t track, uint8_t object, int channel_count, int bit_per_sample, int sample_rate, const void* extra, size_t bytes)
{
    Track::Ptr trace;
    switch (object) 
    {
        case MOV_OBJECT_AAC:
        {
            trace = Factory::getTrackByCodecId(CodecId::CodecAAC, sample_rate, channel_count, bit_per_sample);
            if (extra && bytes)
            {
                trace->setExtraData((uint8_t*)extra, bytes);
            }
            break;
        }
        // add if needed
        // case MOV_OBJECT_OPUS:
        // {
        //     ...
        //     break;
        // }
        default:
            break;
    }
    trace->setIndex(track);
    if (trace/* && _sink */)
    {
        std::lock_guard<std::mutex> sinklock(_sinkMtx);
        _sink->addTrack(trace);
    }
    _audioTrack = track;
    InfoL << "Add audio track, index is: " << track; 
}

SimpleMp4Reader::SimpleMp4Reader(const std::string& vhost, const std::string& app, const std::string& stream_id, const std::string& filePath)
{
    _curTrack = 0;
    _fileSize = 0;
    _vhost = vhost;
    _app = app;
    _streamId = stream_id;
    _filePath = filePath;
    _running = false;
    _videoTrack = 0;
    _audioTrack = 0;
    // Hint : 初略估算
    _videoCacheSize =  30 /* fps */ * 5;
    _audioCacheSize = 50 /* fps */ * 5;
}

SimpleMp4Reader::~SimpleMp4Reader()
{
    Stop();
}

void SimpleMp4Reader::Start()
{
    Stop();
    _sink = std::make_shared<MultiMediaSourceMuxer>(MediaTuple({_vhost, _app, _streamId}));
    _running = true;
    _demuxerThread = std::make_shared<std::thread>([this]()
    {
        _mp4Ifs.open(_filePath);
        if (!_mp4Ifs.is_open())
        {
            _running = false;
            return;
        }
        else
        {
            _mp4Ifs.seekg(0, std::ios::end);
            _fileSize = _mp4Ifs.tellg();
            _mp4Ifs.seekg(0, std::ios::beg);
        }
        struct mov_buffer_t buffer = {
            [](void* param, void* data, uint64_t bytes)
            {
                SimpleMp4Reader* thiz = (SimpleMp4Reader*)param;
                return thiz->read(param, data, bytes);
            },
            [](void *param, const void *data, uint64_t bytes) 
            {
                SimpleMp4Reader* thiz = (SimpleMp4Reader*)param;
                return thiz->write(param, data, bytes);
            },
            [](void *param, int64_t offset) 
            {
                SimpleMp4Reader* thiz = (SimpleMp4Reader*)param;
                return thiz->seek(param, offset);
            },
            [](void *param) 
            {
                SimpleMp4Reader* thiz = (SimpleMp4Reader*)param;
                return thiz->tell(param);
            }
        };
        mov_reader_t* mov = mov_reader_create(&buffer, this);
        mov_reader_trackinfo_t trackInfo = {};
        {
            trackInfo.onvideo = [](void* param, uint32_t track, uint8_t object, int width, int height, const void* extra, size_t bytes)
            {
                SimpleMp4Reader* thiz = (SimpleMp4Reader*)param;
                return thiz->OnVideo(param, track, object, width, height, extra, bytes);
            };
            trackInfo.onaudio = [](void* param, uint32_t track, uint8_t object, int channel_count, int bit_per_sample, int sample_rate, const void* extra, size_t bytes)
            {
                SimpleMp4Reader* thiz = (SimpleMp4Reader*)param;
                return thiz->OnAudio(param, track, object, channel_count, bit_per_sample, sample_rate, extra, bytes);
            };
        }
        mov_reader_getinfo(mov, &trackInfo, this);
        while (_running) 
        {
            if (mov_reader_read2(mov, [](void* param, uint32_t track, size_t bytes, int64_t pts, int64_t dts, int flags){
                SimpleMp4Reader* thiz = (SimpleMp4Reader*)param;
                return thiz->OnMovReaderOnread2(param, track, bytes, pts, dts, flags);
            }, this) <= 0)
            {
                _running = false;
                break;
            }
            else
            {
                if (_curTrack == _videoTrack)
                {
                    if (_curFrame->size() >= 4)
                    // 4 byte frame length to start code
                    {
                        uint8_t* _data = (uint8_t*)_curFrame->data();
                        _data[0] = 0;
                        _data[1] = 0;
                        _data[2] = 0;
                        _data[3] = 1;
                    }
                    std::unique_lock<std::mutex> lock(_videoMtx);
                    if (_videoCache.size() >= _videoCacheSize)
                    {
                        _videoCond.wait(lock);
                    }
                    _videoCache.push_back(_curFrame);
                }
                else if (_curTrack == _audioTrack)
                {
                    std::unique_lock<std::mutex> lock(_audioMtx);
                    if (_audioCache.size() >= _audioCacheSize)
                    {
                        _audioCond.wait(lock);
                    }
                    _audioCache.push_back(_curFrame);
                }
            }
        }
        _mp4Ifs.close();
        mov_reader_destroy(mov);
    });
    _videoTimer = std::make_shared<std::thread>([this]()
    {
        bool frist = true;
        std::chrono::steady_clock::time_point begin;
        while (_running)
        {
            Frame::Ptr frame;
            {
                std::lock_guard<std::mutex> videoMtx(_videoMtx);
                if (!_videoCache.empty())
                {
                    frame = _videoCache.front();
                    _videoCache.pop_front();
                    _videoCond.notify_one();
                }
            }
            if (!frame)
            {
                std::this_thread::sleep_for(std::chrono::milliseconds(1));
                continue; 
            }
            if (frist)
            {
                begin = std::chrono::steady_clock::now() + std::chrono::milliseconds(frame->dts());
                frist = false;
            }
            else
            {
                std::this_thread::sleep_for(std::chrono::milliseconds(frame->dts()) + begin - std::chrono::steady_clock::now());
            }
            {
                std::lock_guard<std::mutex> sinklock(_sinkMtx);
                uint8_t* data = (uint8_t*)frame->data();
                // InfoL << "Video Frame, dts is: " << frame->dts() << " ms";
                _sink->inputFrame(frame);
            }
        }
    });
    _audioTimer = std::make_shared<std::thread>([this]()
    {
        bool frist = true;
        std::chrono::steady_clock::time_point begin;
        while (_running)
        {
            Frame::Ptr frame;
            {
                std::lock_guard<std::mutex> audioMtx(_audioMtx);
                if (!_audioCache.empty())
                {
                    frame = _audioCache.front();
                    _audioCache.pop_front();
                    _audioCond.notify_one();
                }
            }
            if (!frame)
            {
                std::this_thread::sleep_for(std::chrono::milliseconds(1));
                continue; 
            }
            if (frist)
            {
                begin = std::chrono::steady_clock::now() + std::chrono::milliseconds(frame->dts());
                frist = false;
            }
            else
            {
                std::this_thread::sleep_for(std::chrono::milliseconds(frame->dts()) + begin - std::chrono::steady_clock::now());
            }
            {
                std::lock_guard<std::mutex> sinklock(_sinkMtx);
                // InfoL << "Audio Frame, dts is: " << frame->dts() << " ms";
                _sink->inputFrame(frame);
            }
        }
    });
    bool ready = false;
    for (uint32_t i=0; i<300; i++)
    {
        {
            std::lock_guard<std::mutex> sinklock(_sinkMtx);
            ready = _sink->isAllTrackReady();
        }
        if (!ready)
        {
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
        else 
        {
            break;
        }
    }
    if (!ready)
    {
        assert(false);
    }
}

void SimpleMp4Reader::Stop()
{
    if (_running)
    {
        _running = false;
        if (_demuxerThread->joinable())
        {
            _demuxerThread->join();
            _demuxerThread.reset();
        }
        _videoCond.notify_one();
        if (_videoTimer->joinable())
        {
            _videoTimer->join();
            _videoTimer.reset();
        }
        _audioCond.notify_one();
        if (_audioTimer->joinable())
        {
            _audioTimer->join();
            _audioTimer.reset();
        }
    }
    _sink.reset();
}

///////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// 这里才是真正执行main函数，你可以把函数名(domain)改成main，然后就可以输入自定义url了
int domain(const string &file, const string &url) {
    bool simple = false;

    // 设置日志
    Logger::Instance().add(std::make_shared<ConsoleChannel>());
    Logger::Instance().setWriter(std::make_shared<AsyncLogWriter>());

    // 关闭所有转协议
    mINI::Instance()[Protocol::kEnableMP4] = 0;
    mINI::Instance()[Protocol::kEnableFMP4] = 0;
    mINI::Instance()[Protocol::kEnableHls] = 0;
    mINI::Instance()[Protocol::kEnableHlsFmp4] = 0;
    mINI::Instance()[Protocol::kEnableTS] = 0;
    mINI::Instance()[Protocol::kEnableRtsp] = 0;
    mINI::Instance()[Protocol::kEnableRtmp] = 0;

    // 根据url获取媒体协议类型，注意大小写
    auto schema = strToLower(findSubString(url.data(), nullptr, "://").substr(0, 4));

    // 只开启推流协议对应的转协议
    mINI::Instance()["protocol.enable_" + schema] = 1;

    MediaSource::Ptr src = nullptr;

    MP4Reader::Ptr reader;
    SimpleMp4Reader::Ptr readerSimple;

    if (simple)
    {
        // 从mp4文件加载生成MediaSource对象
        reader = std::make_shared<MP4Reader>(DEFAULT_VHOST, "live", "stream", file);
        // 开始加载mp4，ref_self设置为false，这样reader对象设置为nullptr就能注销了，file_repeat可以设置为空，这样文件读完了就停止推流了
        reader->startReadMP4(100, false, true);
    }
    else
    {
        readerSimple = std::make_shared<SimpleMp4Reader>(DEFAULT_VHOST, "live", "stream", file);
        readerSimple->Start();
    }
    src = MediaSource::find(schema, DEFAULT_VHOST, "live", "stream", false);
    if (!src) {
        // 文件不存在
        WarnL << "File not existed: " << file;
        return -1;
    }

    // 选择一个poller线程绑定
    auto poller = EventPollerPool::Instance().getPoller();
    // 创建推流器并绑定一个MediaSource
    auto pusher = std::make_shared<MediaPusher>(src, poller);

    std::weak_ptr<MediaSource> weak_src = src;
    // src用完了，可以直接置空，防止main函数持有它(MP4Reader持有它即可)
    src = nullptr;

    // 可以指定rtsp推流方式，支持tcp和udp方式，默认tcp
    //(*pusher)[Client::kRtpType] = Rtsp::RTP_UDP;

    // 设置推流中断处理逻辑
    std::weak_ptr<MediaPusher> weak_pusher = pusher;
    pusher->setOnShutdown([poller, url, weak_pusher, weak_src](const SockException &ex) {
        if (!weak_src.lock()) {
            // 媒体注销导致的推流中断，不在重试推流
            WarnL << "MediaSource released:" << ex << ", publish stopped";
            return;
        }
        WarnL << "Server connection is closed:" << ex << ", republish after 2 seconds";
        // 重新推流, 2秒后重试
        poller->doDelayTask(2 * 1000, [weak_pusher, url]() {
            if (auto strong_push = weak_pusher.lock()) {
                strong_push->publish(url);
            }
            return 0;
        });
    });

    // 设置发布结果处理逻辑
    pusher->setOnPublished([poller, weak_pusher, url](const SockException &ex) {
        if (!ex) {
            InfoL << "Publish success, please play with player:" << url;
            return;
        }

        WarnL << "Publish fail:" << ex << ", republish after 2 seconds";
        // 如果发布失败，就重试
        poller->doDelayTask(2 * 1000, [weak_pusher, url]() {
            if (auto strong_push = weak_pusher.lock()) {
                strong_push->publish(url);
            }
            return 0;
        });
    });
    pusher->publish(url);

    // sleep(5);
    // reader 置空可以终止推流相关资源
    // reader = nullptr;

    // 设置退出信号处理函数
    static semaphore sem;
    signal(SIGINT, [](int) { sem.post(); }); // 设置退出信号
    sem.wait();
    return 0;
}

int main(int argc, char *argv[]) {
    // 可以使用test_server生成的mp4文件
    // 文件使用绝对路径，推流url支持rtsp和rtmp
    // return domain("/Users/xiongziliang/Downloads/mp4/Quantum.mp4", "rtsp://127.0.0.1/live/rtsp_push");
    return domain(argv[1], argv[2]);
}
