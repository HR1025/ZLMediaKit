/*
 * Copyright (c) 2016-present The ZLMediaKit project authors. All Rights Reserved.
 *
 * This file is part of ZLMediaKit(https://github.com/ZLMediaKit/ZLMediaKit).
 *
 * Use of this source code is governed by MIT-like license that can be found in the
 * LICENSE file in the root of the source tree. All contributing project authors
 * may be found in the AUTHORS file in the root of the source tree.
 */

#include <SDL2/SDL_stdinc.h>
#include <atomic>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <condition_variable>
#include <deque>
#include <fstream>
#include <map>
#include <memory>
#include <mutex>
#include <signal.h>
#include <iostream>
#include <string>
#include <thread>
#include "Common/Device.h"
#include "Common/MediaSink.h"
#include "Common/MediaSource.h"
#include "Extension/Factory.h"
#include "Extension/Frame.h"
#include "Extension/Track.h"
#include "Network/Socket.h"
#include "Util/logger.h"
#include "Util/NoticeCenter.h"
#include "Poller/EventPoller.h"
#include "Player/PlayerProxy.h"
#include "Rtmp/RtmpPusher.h"
#include "Common/config.h"
#include "Pusher/MediaPusher.h"

#include "media-server/libmov/source/mov-internal.h"
#include "mov-format.h"
#include "mov-writer.h"

using namespace std;
using namespace toolkit;
using namespace mediakit;

//////////////////////////////////////////////////////////////////////////////////////////////////////////

//
// 1 write thread
// 异步处理 IO 写入
// 
class SimpleMp4Writer : public MediaSink
{
public:
    using Ptr = std::shared_ptr<SimpleMp4Writer>;
public:
    SimpleMp4Writer(const std::string& filePath);
    ~SimpleMp4Writer();
public:
    void Start();
    void Stop();
private: /* override mov_buffer_t function */
	int read(void* param, void* data, uint64_t bytes);
	int write(void* param, const void* data, uint64_t bytes);
	int seek(void* param, uint64_t offset);
	int64_t tell(void* param);
public:
    void AddTrack(Track::Ptr track);
public: /* override MediaSink event */
    void onAllTrackReady() override;
    bool onTrackFrame(const Frame::Ptr &frame) override;
private:
    std::atomic<bool> _running;
    std::string _filePath;
private:
    std::mutex _writerMtx;
    std::condition_variable _writerCond;
    std::deque<Frame::Ptr> _writerCache;
    std::shared_ptr<thread> _writerThread;
    std::atomic<int> _videoIndex;
    std::atomic<int> _audioIndex;
private:
    std::mutex _writeMtx;
    std::fstream _ofs;
    std::atomic<bool> _initTrack;
    mov_writer_t* _writer;
};

int SimpleMp4Writer::read(void* /* param */, void* data, uint64_t bytes)
{
    _ofs.read((char*)data, bytes);
    return 0;
}

int SimpleMp4Writer::write(void* /* param */, const void* data, uint64_t bytes)
{
    _ofs.write((char*)data, bytes);
    return 0;
}

int SimpleMp4Writer::seek(void* /* param */, uint64_t offset)
{
    _ofs.seekg(offset, std::ios::beg);
    return 0;
}

int64_t SimpleMp4Writer::tell(void* /* param */)
{
    return (int64_t)_ofs.tellg();
}

SimpleMp4Writer::SimpleMp4Writer(const std::string& filePath)
{
    _filePath = filePath;
    _writer = nullptr;
    _videoIndex = 0;
    _audioIndex = 0;
    _initTrack = false;
    InfoL << "SimpleMp4Writer";
}

SimpleMp4Writer::~SimpleMp4Writer()
{
    Stop();
    InfoL << "~SimpleMp4Writer";
}

void SimpleMp4Writer::Start()
{
    _running = true;
    // Hint : 保护 _writer 的时序, 不在异步线程构建对象
    {
        _ofs.open(_filePath, std::ios::out);
        if (!_ofs.is_open())
        {
            _running = false;
            return;
        }
        struct mov_buffer_t buffer = {
        [](void* param, void* data, uint64_t bytes)
        {
            SimpleMp4Writer* thiz = (SimpleMp4Writer*)param;
            return thiz->read(param, data, bytes);
        },
        [](void *param, const void *data, uint64_t bytes) 
        {
            SimpleMp4Writer* thiz = (SimpleMp4Writer*)param;
            return thiz->write(param, data, bytes);
        },
        [](void *param, int64_t offset) 
        {
            SimpleMp4Writer* thiz = (SimpleMp4Writer*)param;
            return thiz->seek(param, offset);
        },
        [](void *param) 
        {
            SimpleMp4Writer* thiz = (SimpleMp4Writer*)param;
            return thiz->tell(param);
        }
        };
        {
            std::lock_guard<std::mutex> writeLock(_writeMtx);
            _writer = mov_writer_create(&buffer, this, 0);
        }
    }
    _writerThread = std::make_shared<std::thread>([this]()
    {
        auto writeFrame = [this](bool& firstFlag, int64_t& delta, Frame::Ptr frame, int64_t& lastDts, int64_t& lastPts, bool isVideo)
        {
            int64_t curDts = frame->dts();
            if (firstFlag)
            {
                if (isVideo && !(frame->keyFrame() || frame->configFrame()))
                {
                    return;
                }
                delta = curDts;
                firstFlag = false;
            }
            int64_t dts = curDts - delta;
            int64_t pts = 0;
            pts = frame->pts() - delta > 0 ? frame->pts() - delta : 0;
            if (dts <= lastDts)
            {
                dts = lastDts + 1;
            }
            if (pts <= lastPts)
            {
                pts = lastPts + 1;
            }
            lastDts = dts;
            lastPts = pts;
            {
                std::lock_guard<std::mutex> writeLock(_writeMtx);
                assert(_writer);
                if (_writer)
                {
                    int flags = 0;
                    uint8_t* data = (uint8_t*)frame->data();
                    size_t bytes = frame->size();
                    toolkit::BufferLikeString buf;
                    if (isVideo)
                    {
                        if (frame->keyFrame())
                        {
                            flags |= MOV_AV_FLAG_KEYFREAME;
                        }
                        buf.resize(4);
                        buf.append((const char*)data + frame->prefixSize(), bytes - frame->prefixSize());
                        {
                            uint8_t* nalu_size_buf = (uint8_t*)buf.data();
                            size_t nalu_size = buf.size() - 4;
                            nalu_size_buf[0] = (uint8_t) ((nalu_size >> 24) & 0xFF);
                            nalu_size_buf[1] = (uint8_t) ((nalu_size >> 16) & 0xFF);
                            nalu_size_buf[2] = (uint8_t) ((nalu_size >> 8) & 0xFF);
                            nalu_size_buf[3] = (uint8_t) ((nalu_size >> 0) & 0xFF);
                        }
                        data = (uint8_t*)buf.data();
                        bytes = buf.size();
                    }
                    else
                    {
                        data += frame->prefixSize();
                        bytes -= frame->prefixSize();
                    }
                    InfoL << "pts is: " << pts << ", dts is: " << dts;
                    mov_writer_write(_writer, isVideo ? _videoIndex : _audioIndex, data, bytes, pts, dts, flags);
                }
            }
        };
        //////////////////////////////////////////////////////////////

        bool canExit = true;
        bool firstVideo = true, firstAudio = true;
        // xxxDelta -> 零同步
        int64_t videoDelta = 0, audioDelta = 0, lastVideoDts = -1, lastAudioDts = -1, lastVideoPts = -1, lastAudioPts = -1;

        while (_running || !canExit)
        {
            Frame::Ptr frame = nullptr;
            {
                std::unique_lock<std::mutex> lock(_writerMtx);
                if (_writerCache.empty())
                {
                    canExit = true;
                    if (!_running)
                    {
                        _writerCond.wait(lock);
                    }
                }
                else
                {
                    canExit = false;
                    frame = _writerCache.front();
                    _writerCache.pop_front();
                }
                if (!frame)
                {
                    continue;
                }
                switch (frame->getTrackType()) 
                {
                    case TrackType::TrackVideo:
                    {
                        writeFrame(firstVideo, videoDelta, frame, lastVideoDts, lastVideoPts, true);
                        break;
                    }
                    case TrackType::TrackAudio:
                    {
                        writeFrame(firstVideo, videoDelta, frame, lastAudioDts, lastVideoDts, false);
                        break;
                    }
                    default:
                        break;
                }
            }
        }
    });
}

void SimpleMp4Writer::Stop()
{
    if (_running)
    {
        _running = false;
        _writerCond.notify_one();
        if (_writerThread->joinable())
        {
            _writerThread->join();
        }
        {
            std::lock_guard<std::mutex> writeLock(_writeMtx);
            mov_writer_destroy(_writer);
        }
        _ofs.flush();
        _ofs.close();
    }
}

void SimpleMp4Writer::AddTrack(Track::Ptr track)
{
    addTrack(track);
}

void SimpleMp4Writer::onAllTrackReady()
{
    // Hint : add if needed
    static std::map<CodecId, uint64_t> kLookup = 
    {
        {CodecId::CodecAAC, MOV_OBJECT_AAC},
        {CodecId::CodecH264, MOV_OBJECT_H264}
    };
    static auto getMovObjectType = [](CodecId id) -> uint64_t
    {
        if (kLookup.count(id))
        {
            return kLookup[id];
        }
        else
        {
            return MOV_OBJECT_NONE;
        }
    };
    if (_initTrack)
    {
        return;
    }
    auto tracks = getTracks();
    for (auto& track : tracks)
{
        auto extra = track->getExtraData();
        auto extra_data = extra ? extra->data() : nullptr;
        auto extra_size = extra ? extra->size() : 0;
        switch (track->getTrackType()) 
        {
            case TrackType::TrackVideo:
            {
                VideoTrack::Ptr videoTrack = std::dynamic_pointer_cast<VideoTrack>(track);
                if (videoTrack)
                {
                    std::lock_guard<std::mutex> lock(_writeMtx);
                    assert(_writer);
                    if (_writer)
                    {
                        _videoIndex = mov_writer_add_video(_writer, getMovObjectType(track->getCodecId()), videoTrack->getVideoWidth(), videoTrack->getVideoHeight(), extra_data, extra_size);
                        InfoL << "Add video track , video index is: " << _videoIndex;
                    }
                }
                break;
            }
            case TrackType::TrackAudio:
            {
                AudioTrack::Ptr audioTrack = std::dynamic_pointer_cast<AudioTrack>(track);
                if (audioTrack)
                {
                    std::lock_guard<std::mutex> lock(_writeMtx);
                    if (_writer)
                    {
                        _audioIndex = mov_writer_add_audio(_writer, getMovObjectType(track->getCodecId()), audioTrack->getAudioChannel(), audioTrack->getAudioSampleBit(), audioTrack->getAudioSampleRate(), extra_data, extra_size);
                        InfoL << "Add audio track , audio index is: " << _audioIndex;
                    }
                    break;
                }
                break;
            }
            default:
            {
                break;
            }
        }
    }
    _initTrack = true;
}

bool SimpleMp4Writer::onTrackFrame(const Frame::Ptr& frame)
{
    std::lock_guard<std::mutex> lock(_writerMtx);
    _writerCache.push_back(Frame::getCacheAbleFrame(frame));
    _writerCond.notify_one();
    return true;
}

//////////////////////////////////////////////////////////////////////////////////////////////////////////

static SimpleMp4Writer::Ptr writer;

//这里才是真正执行main函数，你可以把函数名(domain)改成main，然后就可以输入自定义url了
int domain(const string &playUrl, const string &filePath) {
    bool simple = false;
    Track::Ptr track;
    //设置日志
    Logger::Instance().add(std::make_shared<ConsoleChannel>());
    Logger::Instance().setWriter(std::make_shared<AsyncLogWriter>());
    auto poller = EventPollerPool::Instance().getPoller();

    ProtocolOption option;
    option.enable_hls = false;
    option.enable_mp4 = false;
    PlayerProxy::Ptr player(new PlayerProxy(DEFAULT_VHOST, "app", "stream", option, -1, poller));
    //可以指定rtsp拉流方式，支持tcp和udp方式，默认tcp
//    (*player)[Client::kRtpType] = Rtsp::RTP_UDP;
    player->play(playUrl.data());

    writer = nullptr;
    if (simple)
    {
        // todo:
    }
    else
    {
        std::weak_ptr<mediakit::MediaPlayer> weakPlayer = player;
        writer = std::make_shared<SimpleMp4Writer>(filePath);
        writer->Start();
        std::weak_ptr<SimpleMp4Writer> weakWriter = writer;
        player->setOnPlayResult([weakPlayer, weakWriter](const SockException & ex)
        {
            InfoL << "setOnPlayResult";
            auto player = weakPlayer.lock();
            auto writer = weakWriter.lock();
            if (player && writer && ex.getErrCode() == toolkit::Err_success)
            {
                auto videoTrack = player->getTrack(TrackType::TrackVideo);
                if (videoTrack)
                {
                    writer->AddTrack(Factory::getTrackByCodecId(videoTrack->getCodecId()));
                    videoTrack->addDelegate(writer);
                }
                auto audioTrack = player->getTrack(TrackType::TrackAudio);
                if (audioTrack)
                {
                    writer->AddTrack(Factory::getTrackByCodecId(videoTrack->getCodecId()));
                    audioTrack->addDelegate(writer);
                }
            }
        });
    }
    //设置退出信号处理函数
    static semaphore sem;
    signal(SIGINT, [](int) 
    { 
        if (writer)
        {
            writer->Stop();
            writer.reset();
        }
        sem.post(); 
    });// 设置退出信号
    sem.wait();
    return 0;
}


int main(int argc, char *argv[]) {
    // 拉流地址, 如 rtmp://live.hkstv.hk.lxdns.com/live/hks1
    // 保存mp4文件路径, 如: ./pullMp4.mp4
    return domain(argv[1], argv[2]);
}






