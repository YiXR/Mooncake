#include <gflags/gflags.h>
#include <ylt/coro_rpc/coro_rpc_server.hpp>

#include "client_service.h"
#include "real_client.h"

using namespace mooncake;

DEFINE_string(host, "0.0.0.0", "Local hostname");
DEFINE_string(metadata_server, "http://127.0.0.1:8080/metadata",
              "Metedata server connection string");
DEFINE_int32(port, 50052, "Real Client service port");
DEFINE_string(global_segment_size, "4 GB", "Size of global segment");
DEFINE_int32(threads, 1, "Number of threads for client service");

namespace mooncake {
void RegisterClientRpcService(coro_rpc::coro_rpc_server &server,
                              RealClient &real_client) {
    server.register_handler<&RealClient::put_internal>(&real_client);
    server.register_handler<&RealClient::put_batch_internal>(&real_client);
    server.register_handler<&RealClient::put_parts_internal>(&real_client);
    server.register_handler<&RealClient::remove_internal>(&real_client);
    server.register_handler<&RealClient::removeByRegex_internal>(&real_client);
    server.register_handler<&RealClient::removeAll_internal>(&real_client);
    server.register_handler<&RealClient::isExist_internal>(&real_client);
    server.register_handler<&RealClient::batchIsExist_internal>(&real_client);
    server.register_handler<&RealClient::getSize_internal>(&real_client);
    server.register_handler<&RealClient::get_dummy_buffer_internal>(
        &real_client);
    // server.register_handler<&RealClient::batch_get_buffer>(&real_client);
    // server.register_handler<&RealClient::get_into_internal>(&real_client);
    // server.register_handler<&RealClient::get_hostname>(&real_client);
    server.register_handler<&RealClient::batch_put_from_dummy_internal>(
        &real_client);
    // server.register_handler<&RealClient::put_from_internal>(&real_client);
    server.register_handler<&RealClient::batch_get_into_dummy_internal>(
        &real_client);
    // server.register_handler<&RealClient::put_from_with_metadata>(&real_client);
    // server.register_handler<&RealClient::batch_put_from_multi_buffers_internal>(
    //     &real_client);
    // server.register_handler<&RealClient::batch_get_into_multi_buffers_internal>(
    //     &real_client);
    server.register_handler<&RealClient::map_shm_internal>(&real_client);
    server.register_handler<&RealClient::unmap_shm_internal>(&real_client);
    server.register_handler<&RealClient::register_shm_buffer_internal>(
        &real_client);
    server.register_handler<&RealClient::unregister_shm_buffer_internal>(
        &real_client);
    server.register_handler<&RealClient::service_ready_internal>(&real_client);
}
}  // namespace mooncake

int main(int argc, char *argv[]) {
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    size_t global_segment_size = string_to_byte_size(FLAGS_global_segment_size);

    auto client_inst = RealClient::create();
    auto res = client_inst->setup_internal(FLAGS_host, FLAGS_metadata_server,
                                           global_segment_size, 0, "tcp", "",
                                           "127.0.0.1:50051", nullptr);
    if (!res) {
        LOG(FATAL) << "Failed to setup client: " << toString(res.error());
        return -1;
    }

    coro_rpc::coro_rpc_server server(FLAGS_threads, FLAGS_port, FLAGS_host);
    RegisterClientRpcService(server, *client_inst);

    LOG(INFO) << "Starting client service on " << FLAGS_host << ":"
              << FLAGS_port;

    return server.start();
}
