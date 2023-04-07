// copyright defined in LICENSE.txt

#include "exp_chronos_plugin.hpp"
#include "receiver_plugin.hpp"

#include <iostream>
#include <string>
#include <sstream>

#include <fc/log/logger.hpp>
#include <fc/exception/exception.hpp>

#include <cassandra.h>

using namespace chronicle::channels;
using namespace abieos;

using std::make_shared;
using std::string;

static auto _exp_chronos_plugin = app().register_plugin<exp_chronos_plugin>();

namespace {
  const char* SCYLLA_HOSTS_OPT = "scylla-hosts";
  const char* SCYLLA_PORT_OPT = "scylla-port";
  const char* SCYLLA_KEYSPACE_OPT = "scylla-keyspace";
  const char* SCYLLA_CONNS_PER_HOST_OPT = "scylla-connections-per-host";
  const char* SCYLLA_USERNAME_OPT = "scylla-username";
  const char* SCYLLA_PASSWORD_OPT = "scylla-password";
  const char* SCYLLA_CONSISTENCY_OPT = "scylla-consistency";

  const char* CHRONOS_MAXUNACK_OPT = "chronos-max-unack";
}


class exp_chronos_plugin_impl : std::enable_shared_from_this<exp_chronos_plugin_impl> {
public:
  exp_chronos_plugin_impl()
  {}

  string scylla_hosts;
  uint16_t scylla_port;
  string scylla_keyspace;
  uint16_t scylla_conn_per_host;
  string scylla_username;
  string scylla_password;

  uint32_t maxunack;

  CassCluster* cluster;
  CassSession* session;

  bool is_bootstrapping = false;

  chronicle::channels::forks::channel_type::handle               _forks_subscription;
  chronicle::channels::transaction_traces::channel_type::handle  _transaction_traces_subscription;
  chronicle::channels::abi_removals::channel_type::handle        _abi_removals_subscription;
  chronicle::channels::abi_updates::channel_type::handle         _abi_updates_subscription;
  chronicle::channels::block_completed::channel_type::handle     _block_completed_subscription;

  const int channel_priority = 55;

  void start() {
    exporter_will_ack_blocks(maxunack);

    _forks_subscription =
      app().get_channel<chronicle::channels::forks>().subscribe
      ([this](std::shared_ptr<chronicle::channels::fork_event> fe){
        on_fork(fe);
      });

    _transaction_traces_subscription =
      app().get_channel<chronicle::channels::transaction_traces>().subscribe
      ([this](std::shared_ptr<chronicle::channels::transaction_trace> tr){
        on_transaction_trace(tr);
      });

    _abi_updates_subscription =
      app().get_channel<chronicle::channels::abi_updates>().subscribe
      ([this](std::shared_ptr<chronicle::channels::abi_update> abiupd){
        on_abi_update(abiupd);
      });

    _abi_removals_subscription =
      app().get_channel<chronicle::channels::abi_removals>().subscribe
      ([this](std::shared_ptr<chronicle::channels::abi_removal> ar){
        on_abi_removal(ar);
      });

    _block_completed_subscription =
      app().get_channel<chronicle::channels::block_completed>().subscribe
      ([this](std::shared_ptr<block_finished> bf){
        on_block_completed(bf);
      });

    cluster = cass_cluster_new();
    session = cass_session_new();
    cass_cluster_set_contact_points(cluster, scylla_hosts.c_str());
    cass_cluster_set_local_port_range(cluster, 49152, 65535);
    cass_cluster_set_core_connections_per_host(cluster, scylla_conn_per_host);

    if( scylla_username.size() > 0 ) {
      cass_cluster_set_credentials(cluster, scylla_username.c_str(), scylla_password.c_str());
    }

    CassFuture* future;

    ilog("Connecting to ScyllaDB cluster: ${h}", ("h",scylla_hosts));
    future = cass_session_connect_keyspace(session, cluster, scylla_keyspace.c_str());
    check_future(future, "connecting to ScyllaDB cluster");
    cass_future_free(future);

    {
      CassStatement* statement = cass_statement_new("SELECT ptr FROM pointers WHERE id=2", 0);
      future = cass_session_execute(session, statement);
      check_future(future, "quering");
      const CassResult* result = cass_future_get_result(future);
      if( cass_result_row_count(result) == 0 ) {
        is_bootstrapping = true;
      }
      cass_result_free(result);
      cass_statement_free(statement);
      cass_future_free(future);
    }

    if( is_bootstrapping ) {
      ilog("Bootstrapping an empty database: writing all known ABI history");
    }

  }

  void check_future(CassFuture* future, const string what) {
    if (cass_future_error_code(future) != CASS_OK) {
      const char* message;
      size_t message_length;
      cass_future_error_message(future, &message, &message_length);
      string msg(message, message_length);
      elog("Error " + what + ": ${e}", ("e",msg));
      throw std::runtime_error(msg);
    }
  }


  void on_fork(std::shared_ptr<chronicle::channels::fork_event> fe) {
  }

  void on_transaction_trace(std::shared_ptr<chronicle::channels::transaction_trace> ccttr) {
  }

  void on_abi_update(std::shared_ptr<chronicle::channels::abi_update> abiupd) {
  }

  void on_abi_removal(std::shared_ptr<chronicle::channels::abi_removal> ar) {
  }

  void on_block_completed(std::shared_ptr<block_finished> bf) {
  }

};



exp_chronos_plugin::exp_chronos_plugin() :my(new exp_chronos_plugin_impl){
}

exp_chronos_plugin::~exp_chronos_plugin(){
}


void exp_chronos_plugin::set_program_options( options_description& cli, options_description& cfg ) {
  cfg.add_options()
    (SCYLLA_HOSTS_OPT, bpo::value<string>(), "Comma-separated ScyllaDB server hosts")
    (SCYLLA_PORT_OPT, bpo::value<uint16_t>()->default_value(9042), "ScyllaDB server port")
    (SCYLLA_KEYSPACE_OPT, bpo::value<string>()->default_value("chronos"), "ScyllaDB keyspace")
    (SCYLLA_CONNS_PER_HOST_OPT, bpo::value<uint16_t>()->default_value(32), "Number of connections per host")
    (SCYLLA_USERNAME_OPT, bpo::value<string>(), "ScyllaDB authentication name")
    (SCYLLA_PASSWORD_OPT, bpo::value<string>(), "ScyllaDB authentication password")
    (SCYLLA_CONSISTENCY_OPT, bpo::value<uint16_t>()->default_value(CASS_CONSISTENCY_QUORUM), "Cluster consistency level")
    (CHRONOS_MAXUNACK_OPT, bpo::value<uint32_t>()->default_value(1000),
     "Receiver will pause at so many unacknowledged blocks")
    ;
}


void exp_chronos_plugin::plugin_initialize( const variables_map& options ) {
  if (is_noexport_opt(options))
    return;

  try {
    donot_start_receiver_before(this, "exp_chronos_plugin");
    ilog("Initialized exp_chronos_plugin");

    bool opt_missing = false;
    if( options.count(SCYLLA_HOSTS_OPT) != 1 ) {
      elog("${o} not specified, as required by exp_chronos_plugin", ("o",SCYLLA_HOSTS_OPT));
      opt_missing = true;
    }

    my->scylla_hosts = options.at(SCYLLA_HOSTS_OPT).as<string>();
    my->scylla_port = options.at(SCYLLA_PORT_OPT).as<uint16_t>();
    my->scylla_keyspace = options.at(SCYLLA_KEYSPACE_OPT).as<string>();
    my->scylla_conn_per_host = options.at(SCYLLA_CONNS_PER_HOST_OPT).as<uint16_t>();


    if( options.count(SCYLLA_USERNAME_OPT) != 0 ) {
      my->scylla_username = options.at(SCYLLA_USERNAME_OPT).as<string>();
      if( options.count(SCYLLA_PASSWORD_OPT) != 1 ) {
        elog("${o} missing", ("o",SCYLLA_PASSWORD_OPT));
        opt_missing = true;
      }
      else {
        my->scylla_password = options.at(SCYLLA_PASSWORD_OPT).as<string>();
      }
    }

    my->maxunack = options.at(CHRONOS_MAXUNACK_OPT).as<uint32_t>();
    if( my->maxunack == 0 )
      throw std::runtime_error("Maximum unacked blocks must be a positive integer");

    if( opt_missing )
      throw std::runtime_error("Mandatory option missing");
  }
  FC_LOG_AND_RETHROW();
}


void exp_chronos_plugin::plugin_startup(){
  if (!is_noexport_mode()) {
    my->start();
    ilog("Started exp_chronos_plugin");
  }
}

void exp_chronos_plugin::plugin_shutdown() {
  if (!is_noexport_mode()) {
    ilog("exp_chronos_plugin stopped");
  }
}
