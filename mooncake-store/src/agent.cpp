#include <glog/logging.h>
#include <future>
#include <chrono>

#include "agent_service.h"
#include "ha_helper.h"
#include "types.h"
#include "segment.h"

namespace mooncake {

/**
 * @brief Re-mounts all segments currently tracked by the agent.
 *
 * This function is called by the PingThread after a master failure/restart
 * is detected (indicated by NEED_REMOUNT status). It reads the agent's
 * internal record of mounted segments and sends a ReMountSegment RPC
 * to the master to restore its state.
 */
void AgentService::ReMountAllSegments() {
    std::vector<Segment> segments_to_remount;
    {
        // Lock the mutex to get a consistent snapshot of the mounted segments
        std::lock_guard<std::mutex> lock(mounted_segments_mutex_);
        if (mounted_segments_.empty()) {
            LOG(INFO) << "Agent PingThread: NEED_REMOUNT received, but no "
                         "segments are currently tracked.";
            return;
        }

        LOG(INFO) << "Agent PingThread: Re-mounting "
                  << mounted_segments_.size() << " segments...";

        // Copy segment info into a new vector to be used outside the lock
        segments_to_remount.reserve(mounted_segments_.size());
        for (auto const& [id, segment] : mounted_segments_) {
            segments_to_remount.push_back(segment);
        }
    }

    // Perform the RPC call outside the lock to avoid blocking
    auto remount_result = master_client_.ReMountSegment(segments_to_remount);
    if (!remount_result) {
        ErrorCode err = remount_result.error();
        LOG(ERROR) << "Agent failed to remount segments: " << toString(err);
    } else {
        LOG(INFO) << "Agent successfully remounted "
                  << segments_to_remount.size() << " segments.";
    }
}

/**
 * @brief Main loop for the agent's background ping thread.
 *
 * 1. Periodically ping the master to maintain the connection.
 * 2. Handle HA failover by contacting etcd for a new master view.
 * 3. Handle non-HA reconnection attempts.
 * 4. Trigger ReMountAllSegments when the master indicates a state reset.
 */
void AgentService::PingThreadMain(bool is_ha_mode,
                                  std::string current_master_address) {
    // How many failed pings before getting latest master view from etcd
    const int max_ping_fail_count = 3;
    // How long to wait for next ping after success
    const int success_ping_interval_ms = 1000;
    // How long to wait for next ping after failure
    const int fail_ping_interval_ms = 1000;
    // Increment after a ping failure, reset after a ping success
    int ping_fail_count = 0;

    // Future to manage the async remount task, ensuring only one runs at a time
    std::future<void> remount_segments_future;

    while (ping_running_) {
        if (remount_segments_future.valid() &&
            remount_segments_future.wait_for(std::chrono::seconds(0)) ==
                std::future_status::ready) {
            try {
                remount_segments_future.get();
            } catch (const std::exception& e) {
                LOG(ERROR) << "Remount segments task failed with exception: "
                           << e.what();
            }
            remount_segments_future = std::future<void>();
        }

        // Ping master
        auto ping_result = master_client_.Ping();
        if (ping_result) {
            // Reset ping failure count
            ping_fail_count = 0;
            auto& ping_response = ping_result.value();

            if (ping_response.client_status == ClientStatus::NEED_REMOUNT &&
                !remount_segments_future.valid()) {
                // Ensure at most one remount segment thread is running
                remount_segments_future =
                    std::async(std::launch::async,
                               [this]() { this->ReMountAllSegments(); });
            }

            std::this_thread::sleep_for(
                std::chrono::milliseconds(success_ping_interval_ms));
            continue;
        }

        ping_fail_count++;
        if (ping_fail_count < max_ping_fail_count) {
            LOG(ERROR) << "Agent failed to ping master (count="
                       << ping_fail_count << ")";
            std::this_thread::sleep_for(
                std::chrono::milliseconds(fail_ping_interval_ms));
            continue;
        }

        // Exceeded ping failure threshold. Attempt to reconnect.
        if (is_ha_mode) {
            LOG(ERROR)
                << "Agent failed to ping master for " << ping_fail_count
                << " times; fetching latest master view and reconnecting";
            std::string master_address;
            ViewVersionId next_version = 0;
            auto err =
                master_view_helper_.GetMasterView(master_address, next_version);
            if (err != ErrorCode::OK) {
                LOG(ERROR) << "Agent failed to get new master view: "
                           << toString(err);
                std::this_thread::sleep_for(
                    std::chrono::milliseconds(fail_ping_interval_ms));
                continue;
            }

            err = master_client_.Connect(master_address);
            if (err != ErrorCode::OK) {
                LOG(ERROR) << "Agent failed to connect to new master "
                           << master_address << ": " << toString(err);
                std::this_thread::sleep_for(
                    std::chrono::milliseconds(fail_ping_interval_ms));
                continue;
            }

            current_master_address = master_address;
            LOG(INFO) << "Agent reconnected to new master " << master_address;
            ping_fail_count = 0;
        } else {
            LOG(ERROR) << "Agent failed to ping master for " << ping_fail_count
                       << " times (non-HA); reconnecting to "
                       << current_master_address;
            auto err = master_client_.Connect(current_master_address);
            if (err != ErrorCode::OK) {
                LOG(ERROR) << "Agent reconnect failed to "
                           << current_master_address << ": " << toString(err);
                std::this_thread::sleep_for(
                    std::chrono::milliseconds(fail_ping_interval_ms));
                continue;
            }
            LOG(INFO) << "Agent reconnected to master "
                      << current_master_address;
            ping_fail_count = 0;
        }
    }
    // Explicitly wait for the remount segment thread to finish
    if (remount_segments_future.valid()) {
        remount_segments_future.wait();
    }
}

}  // namespace mooncake