#pragma once

#include <glog/logging.h>
#include <atomic>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>
#include <ylt/coro_rpc/coro_rpc_server.hpp>
#include <ylt/util/tl/expected.hpp>

#include "ha_helper.h"
#include "master_client.h"
#include "replica.h"
#include "rpc_types.h"
#include "segment.h"
#include "types.h"

namespace mooncake {

/**
 * @brief Defines the gRPC service run by the standalone Mooncake Agent process.
 *
 * This service encapsulates the MasterClient and all stateful interactions
 * with the Mooncake Master (e.g., connection, HA, ping thread, mounted
 * segments). The main 'Client' stub (running in the user application)
 * communicates with this Agent via IPC (e.g., UNIX Domain Sockets)
 * to perform metadata operations.
 */
class AgentService {
   public:
    /**
     * @brief Constructs the AgentService.
     * @param master_server_entry The connection string for the Mooncake Master
     * (e.g., "127.0.0.1:50051" or "etcd://...")
     */
    AgentService(const std::string& master_server_entry)
        : client_id_(
              generate_uuid()),  // This agent has its own persistent UUID
          master_client_(client_id_, nullptr) {
        LOG(INFO) << "Mooncake Agent starting with agent_id=" << client_id_;
        LOG(INFO) << "Connecting to Master at " << master_server_entry;

        auto err = master_client_.Connect(master_server_entry);
        if (err != ErrorCode::OK) {
            LOG(FATAL) << "AgentService failed to connect to master: "
                       << toString(err);
        }

        LOG(INFO) << "AgentService connected to master.";

        // Start the persistent ping thread within the agent process
        StartPingThread(master_server_entry.find("etcd://") == 0,
                        master_server_entry);
    }

    /**
     * @brief Destroys the AgentService, stopping the ping thread.
     */
    ~AgentService() {
        if (ping_running_) {
            ping_running_ = false;
            if (ping_thread_.joinable()) {
                ping_thread_.join();
            }
        }
        LOG(INFO) << "Mooncake Agent shut down.";
    }

    async_simple::coro::Lazy<tl::expected<bool, ErrorCode>> ExistKey(
        std::string object_key) {
        co_return master_client_.ExistKey(object_key);
    }

    async_simple::coro::Lazy<std::vector<tl::expected<bool, ErrorCode>>>
    BatchExistKey(std::vector<std::string> object_keys) {
        co_return master_client_.BatchExistKey(object_keys);
    }

    async_simple::coro::Lazy<tl::expected<GetReplicaListResponse, ErrorCode>>
    GetReplicaList(std::string object_key) {
        co_return master_client_.GetReplicaList(object_key);
    }

    async_simple::coro::Lazy<tl::expected<
        std::unordered_map<std::string, std::vector<Replica::Descriptor>>,
        ErrorCode>>
    GetReplicaListByRegex(std::string str) {
        co_return master_client_.GetReplicaListByRegex(str);
    }

    async_simple::coro::Lazy<
        std::vector<tl::expected<GetReplicaListResponse, ErrorCode>>>
    BatchGetReplicaList(std::vector<std::string> object_keys) {
        co_return master_client_.BatchGetReplicaList(object_keys);
    }

    async_simple::coro::Lazy<
        tl::expected<std::vector<Replica::Descriptor>, ErrorCode>>
    PutStart(std::string key, std::vector<uint64_t> slice_lengths_rpc,
             ReplicateConfig config) {
        // Convert from RPC-safe uint64_t back to size_t for MasterClient call
        std::vector<size_t> slice_lengths;
        slice_lengths.reserve(slice_lengths_rpc.size());
        for (auto l : slice_lengths_rpc) {
            slice_lengths.push_back(static_cast<size_t>(l));
        }
        co_return master_client_.PutStart(key, slice_lengths, config);
    }

    async_simple::coro::Lazy<
        std::vector<tl::expected<std::vector<Replica::Descriptor>, ErrorCode>>>
    BatchPutStart(std::vector<std::string> keys,
                  std::vector<std::vector<uint64_t>> slice_lengths,
                  ReplicateConfig config) {
        co_return master_client_.BatchPutStart(keys, slice_lengths, config);
    }

    async_simple::coro::Lazy<tl::expected<void, ErrorCode>> PutEnd(
        std::string key, ReplicaType replica_type) {
        co_return master_client_.PutEnd(key, replica_type);
    }

    async_simple::coro::Lazy<std::vector<tl::expected<void, ErrorCode>>>
    BatchPutEnd(std::vector<std::string> keys) {
        co_return master_client_.BatchPutEnd(keys);
    }

    async_simple::coro::Lazy<tl::expected<void, ErrorCode>> PutRevoke(
        std::string key, ReplicaType replica_type) {
        co_return master_client_.PutRevoke(key, replica_type);
    }

    async_simple::coro::Lazy<std::vector<tl::expected<void, ErrorCode>>>
    BatchPutRevoke(std::vector<std::string> keys) {
        co_return master_client_.BatchPutRevoke(keys);
    }

    async_simple::coro::Lazy<tl::expected<void, ErrorCode>> Remove(
        std::string key) {
        co_return master_client_.Remove(key);
    }

    async_simple::coro::Lazy<tl::expected<long, ErrorCode>> RemoveByRegex(
        std::string str) {
        co_return master_client_.RemoveByRegex(str);
    }

    async_simple::coro::Lazy<tl::expected<long, ErrorCode>> RemoveAll() {
        co_return master_client_.RemoveAll();
    }

    /**
     * @brief Mounts a segment to the Master and tracks it within the Agent.
     * The Client stub is responsible for registering the memory with its
     * own TransferEngine, but the Agent is responsible for telling the Master
     * that this segment exists.
     */
    async_simple::coro::Lazy<tl::expected<void, ErrorCode>> MountSegment(
        Segment segment) {
        auto result = master_client_.MountSegment(segment);
        if (result) {
            std::lock_guard<std::mutex> lock(mounted_segments_mutex_);
            mounted_segments_[segment.id] = segment;
            LOG(INFO) << "Agent tracked new segment mount: " << segment.id;
        }
        co_return result;
    }

    /**
     * @brief Unmounts a segment from the Master and stops tracking it.
     */
    async_simple::coro::Lazy<tl::expected<void, ErrorCode>> UnmountSegment(
        UUID segment_id) {
        auto result = master_client_.UnmountSegment(segment_id);
        if (result) {
            std::lock_guard<std::mutex> lock(mounted_segments_mutex_);
            if (mounted_segments_.erase(segment_id)) {
                LOG(INFO) << "Agent untracked segment: " << segment_id;
            }
        }
        co_return result;
    }

    async_simple::coro::Lazy<tl::expected<std::string, ErrorCode>> GetFsdir() {
        co_return master_client_.GetFsdir();
    }

   private:
    /**
     * @brief Starts the background thread for pinging the master.
     */
    void StartPingThread(bool is_ha_mode, std::string current_master_address) {
        ping_running_ = true;
        ping_thread_ = std::thread([this, is_ha_mode,
                                    current_master_address]() mutable {
            this->PingThreadMain(is_ha_mode, std::move(current_master_address));
        });
    }

    /**
     * @brief Main loop for the ping thread for high availability
     */
    void PingThreadMain(bool is_ha_mode, std::string current_master_address);

    /**
     * @brief Called by PingThreadMain when a NEED_REMOUNT status is detected.
     * This re-registers all tracked segments with the Master.
     */
    void ReMountAllSegments();

    // Client identification
    const UUID client_id_;
    MasterClient master_client_;
    MasterViewHelper master_view_helper_;

    std::thread ping_thread_;
    std::atomic<bool> ping_running_{false};

    // Track mounted segments to handle remounts
    std::mutex mounted_segments_mutex_;
    std::unordered_map<UUID, Segment, boost::hash<UUID>> mounted_segments_
        GUARDED_BY(mounted_segments_mutex_);
};

}  // namespace mooncake