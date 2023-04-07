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


// tracker for ScyllaDB asynchronous requests
struct req_queue_entry {
  uint32_t block_num;
  uint32_t req_counter;
};


void scylla_result_callback(CassFuture* future, void* data)
{
  if (cass_future_error_code(future) != CASS_OK) {
    const char* message;
    size_t message_length;
    cass_future_error_message(future, &message, &message_length);
    string msg(message, message_length);
    elog("Error: ${e}", ("e",msg));
    abort_receiver();
  }

  ((req_queue_entry*)data)->req_counter--;
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

  const CassPrepared* prepared_ins_pointers;
  const CassPrepared* prepared_ins_transactions;
  const CassPrepared* prepared_ins_receipts;
  const CassPrepared* prepared_ins_recv_sequence_max;
  const CassPrepared* prepared_ins_actions;
  const CassPrepared* prepared_ins_abi_history;

  const CassPrepared* prepared_del_transactions;
  const CassPrepared* prepared_del_receipts;
  const CassPrepared* prepared_del_actions;
  const CassPrepared* prepared_del_abi_history;

  chronicle::channels::forks::channel_type::handle               _forks_subscription;
  chronicle::channels::block_started::channel_type::handle       _block_started_subscription;
  chronicle::channels::transaction_traces::channel_type::handle  _transaction_traces_subscription;
  chronicle::channels::abi_removals::channel_type::handle        _abi_removals_subscription;
  chronicle::channels::abi_updates::channel_type::handle         _abi_updates_subscription;
  chronicle::channels::block_completed::channel_type::handle     _block_completed_subscription;

  const int channel_priority = 55;

  std::queue<req_queue_entry> request_queue;

  void start() {
    exporter_will_ack_blocks(maxunack);

    _forks_subscription =
      app().get_channel<chronicle::channels::forks>().subscribe
      ([this](std::shared_ptr<chronicle::channels::fork_event> fe){
        on_fork(fe);
      });

    _block_started_subscription =
      app().get_channel<chronicle::channels::block_started>().subscribe
      ([this](std::shared_ptr<block_begins> bb){
        on_block_started(bb);
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


    future = cass_session_prepare
      (session, "INSERT INTO pointers (id, ptr) VALUES (?,?)");
    check_future(future, "preparing statement");
    prepared_ins_pointers = cass_future_get_prepared(future);
    cass_future_free(future);


    future = cass_session_prepare
      (session, "INSERT INTO transactions (block_num, block_time, seq, trx_id, trace) VALUES (?,?,?,?,?)");
    check_future(future, "preparing statement");
    prepared_ins_transactions = cass_future_get_prepared(future);
    cass_future_free(future);


    future = cass_session_prepare
      (session,
       "INSERT INTO receipts (block_num, block_time, seq, account_name, recv_sequence_start, recv_sequence_count) VALUES (?,?,?,?,?,?)");
    check_future(future, "preparing statement");
    prepared_ins_receipts = cass_future_get_prepared(future);
    cass_future_free(future);


    future = cass_session_prepare
      (session, "INSERT INTO actions (block_num, block_time, seq, contract, action) VALUES (?,?,?,?,?)");
    check_future(future, "preparing statement");
    prepared_ins_actions = cass_future_get_prepared(future);
    cass_future_free(future);


    future = cass_session_prepare
      (session,
       "INSERT INTO recv_sequence_max (account_name, recv_sequence_max) VALUES (?,?)");
    check_future(future, "preparing statement");
    prepared_ins_recv_sequence_max = cass_future_get_prepared(future);
    cass_future_free(future);


    future = cass_session_prepare
      (session, "INSERT INTO abi_history (block_num, account_name, abi_raw) VALUES (?,?,?)");
    check_future(future, "preparing statement");
    prepared_ins_abi_history = cass_future_get_prepared(future);
    cass_future_free(future);


    future = cass_session_prepare(session, "DELETE FROM transactions WHERE block_num=?");
    check_future(future, "preparing statement");
    prepared_del_transactions = cass_future_get_prepared(future);
    cass_future_free(future);

    future = cass_session_prepare(session, "DELETE FROM receipts WHERE block_num=?");
    check_future(future, "preparing statement");
    prepared_del_receipts = cass_future_get_prepared(future);
    cass_future_free(future);

    future = cass_session_prepare(session, "DELETE FROM actions WHERE block_num=?");
    check_future(future, "preparing statement");
    prepared_del_actions = cass_future_get_prepared(future);
    cass_future_free(future);

    future = cass_session_prepare(session, "DELETE FROM abi_history WHERE block_num=?");
    check_future(future, "preparing statement");
    prepared_del_abi_history = cass_future_get_prepared(future);
    cass_future_free(future);


    if( is_bootstrapping ) {
      ilog("Bootstrapping an empty database: writing all known ABI history");
      uint32_t count = 0;

      CassStatement* statement = cass_prepared_bind(prepared_ins_abi_history);

      receiver_plug->walk_abi_history([&](uint64_t account, uint32_t block_index,
                                             const char* abi_data, size_t abi_size) {
        size_t pos = 0;
        cass_statement_bind_int64(statement, pos++, block_index);
        cass_statement_bind_string(statement, pos++, eosio::name_to_string(account).c_str());
        cass_statement_bind_bytes(statement, pos++, (cass_byte_t*) abi_data, abi_size);
        future = cass_session_execute(session, statement);
        check_future(future, "inserting abi_history");
        cass_future_free(future);
        ++count;
      });

      cass_statement_free(statement);
      ilog("Wrote ${c} abi_history rows", ("c",count));
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
    ack_block(fe->block_num - 1);
  }

  void on_block_started(std::shared_ptr<chronicle::channels::block_begins> bb) {
    request_queue.push({.block_num=bb->block_num, .req_counter=0});

    CassStatement* statement = cass_prepared_bind(prepared_ins_pointers);
    size_t pos = 0;
    cass_statement_bind_int32(statement, pos++, 0); // id=0: last written block
    cass_statement_bind_int64(statement, pos++, bb->block_num); // id=0: last written block

    req_queue_entry& tracker = request_queue.back();
    ++tracker.req_counter;

    CassFuture* future = cass_session_execute(session, statement);
    cass_future_set_callback(future, scylla_result_callback, &tracker);
    cass_future_free(future);
    cass_statement_free(statement);
  }

  void on_transaction_trace(std::shared_ptr<chronicle::channels::transaction_trace> ccttr) {
    auto& trace = std::get<eosio::ship_protocol::transaction_trace_v0>(ccttr->trace);

    uint64_t global_seq = 0;
    uint64_t block_timestamp = ccttr->block_timestamp.to_time_point().elapsed.count() / 1000;

    std::map<uint64_t, uint64_t> recv_seq_start;
    std::map<uint64_t, uint64_t> recv_seq_max;
    std::map<uint64_t, std::set<uint64_t>> actions_seen;

    for( auto atrace = trace.action_traces.begin();
         atrace != trace.action_traces.end();
         ++atrace ) {

      eosio::ship_protocol::action*               act;
      eosio::ship_protocol::action_receipt_v0*    receipt;
      eosio::name                                 receiver;

      size_t index = atrace->index();
      if( index == 0 ) {
        eosio::ship_protocol::action_trace_v0& at = std::get<eosio::ship_protocol::action_trace_v0>(*atrace);
        if( ! at.receipt ) {
          break;
        }
        act = &at.act;
        receipt = &(std::get<eosio::ship_protocol::action_receipt_v0>(at.receipt.value()));
        receiver = at.receiver;
      }
      else if( index == 1 ) {
        eosio::ship_protocol::action_trace_v1& at = std::get<eosio::ship_protocol::action_trace_v1>(*atrace);
        if( ! at.receipt ) {
          break;
        }
        act = &at.act;
        receipt = &(std::get<eosio::ship_protocol::action_receipt_v0>(at.receipt.value()));
        receiver = at.receiver;
      }
      else {
        throw std::runtime_error(string("Invalid variant option in action_trace: ") + std::to_string(index));
      }

      if( global_seq == 0 ) {
        global_seq = receipt->global_sequence;
      }

      if( recv_seq_start.count(receiver.value) == 0 ) {
        recv_seq_start.emplace(receiver.value, receipt->recv_sequence);
      }

      recv_seq_max.insert_or_assign(receiver.value, receipt->recv_sequence);

      eosio::name contract = act->account;
      if( receiver == contract ) {
        actions_seen[contract.value].insert(act->name.value);
      }
    }

    if( global_seq == 0 ) {
      throw std::runtime_error("global_seq is zero");
    }

    auto trx_id = trace.id.extract_as_byte_array();

    CassStatement* statement = cass_prepared_bind(prepared_ins_transactions);
    size_t pos = 0;
    cass_statement_bind_int64(statement, pos++, ccttr->block_num);
    cass_statement_bind_int64(statement, pos++, block_timestamp);
    cass_statement_bind_int64(statement, pos++, global_seq);
    cass_statement_bind_bytes(statement, pos++, (cass_byte_t*)trx_id.data(), trx_id.size());
    cass_statement_bind_bytes(statement, pos++, (cass_byte_t*)ccttr->bin_start, ccttr->bin_size);

    req_queue_entry& tracker = request_queue.back();
    ++tracker.req_counter;

    CassFuture* future = cass_session_execute(session, statement);
    cass_future_set_callback(future, scylla_result_callback, &tracker);
    cass_future_free(future);
    cass_statement_free(statement);

    statement = cass_prepared_bind(prepared_ins_receipts);
    for(auto item: recv_seq_start) {
      size_t pos = 0;
      cass_statement_bind_int64(statement, pos++, ccttr->block_num);
      cass_statement_bind_int64(statement, pos++, block_timestamp);
      cass_statement_bind_int64(statement, pos++, global_seq);
      cass_statement_bind_string(statement, pos++, eosio::name_to_string(item.first).c_str());
      cass_statement_bind_int64(statement, pos++, item.second);
      cass_statement_bind_int64(statement, pos++, recv_seq_max.at(item.first) - item.second + 1);

      ++tracker.req_counter;

      future = cass_session_execute(session, statement);
      cass_future_set_callback(future, scylla_result_callback, &tracker);
      cass_future_free(future);
    }
    cass_statement_free(statement);


    statement = cass_prepared_bind(prepared_ins_recv_sequence_max);
    for(auto item: recv_seq_max) {
      size_t pos = 0;
      cass_statement_bind_string(statement, pos++, eosio::name_to_string(item.first).c_str());
      cass_statement_bind_int64(statement, pos++, item.second);

      ++tracker.req_counter;

      future = cass_session_execute(session, statement);
      cass_future_set_callback(future, scylla_result_callback, &tracker);
      cass_future_free(future);
    }
    cass_statement_free(statement);

    statement = cass_prepared_bind(prepared_ins_actions);
    for(auto item: actions_seen) {
      for(auto aname: item.second) {
        size_t pos = 0;
        cass_statement_bind_int64(statement, pos++, ccttr->block_num);
        cass_statement_bind_int64(statement, pos++, block_timestamp);
        cass_statement_bind_int64(statement, pos++, global_seq);
        cass_statement_bind_string(statement, pos++, eosio::name_to_string(item.first).c_str());
        cass_statement_bind_string(statement, pos++, eosio::name_to_string(aname).c_str());

        ++tracker.req_counter;

        future = cass_session_execute(session, statement);
        cass_future_set_callback(future, scylla_result_callback, &tracker);
        cass_future_free(future);
      }
    }
    cass_statement_free(statement);
  }

  void on_abi_update(std::shared_ptr<chronicle::channels::abi_update> abiupd) {
  }

  void on_abi_removal(std::shared_ptr<chronicle::channels::abi_removal> ar) {
  }

  void on_block_completed(std::shared_ptr<block_finished> bf) {
    uint32_t ack = 0;
    while( request_queue.front().req_counter == 0 ) {
      ack = request_queue.front().block_num;
      request_queue.pop();
    }

    if( ack > 0 ) {
      ack_block(ack);
    }
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
