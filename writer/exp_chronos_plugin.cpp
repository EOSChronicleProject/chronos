// copyright defined in LICENSE.txt

#include "exp_chronos_plugin.hpp"
#include "receiver_plugin.hpp"

#include <iostream>
#include <string>
#include <sstream>
#include <boost/thread/mutex.hpp>
#include <boost/asio/deadline_timer.hpp>
#include <boost/thread.hpp>

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

  const uint64_t MILLISECONDS_IN_A_DAY = 24 * 3600 * 1000;
}

// scylla client threads may exit after main thread, so we we need to organize a clean aborting

static boost::mutex queue_mtx;
static bool is_exiting = false;

void atexit_handler()
{
  is_exiting = true;
}

// tracker for ScyllaDB asynchronous requests
struct req_queue_entry {
  uint32_t block_num;
  uint64_t block_timestamp;
  uint64_t block_date;
  uint32_t trace_jobs_counter = 0;
  uint32_t db_req_counter = 0;
  uint32_t total_trx = 0;

  inline bool is_finished() const {return (trace_jobs_counter == 0 && db_req_counter == 0);}
};


void scylla_result_callback(CassFuture* future, void* data)
{
  if( !is_exiting ) {
    if (cass_future_error_code(future) != CASS_OK) {
      const char* message;
      size_t message_length;
      cass_future_error_message(future, &message, &message_length);
      string msg(message, message_length);
      elog("Error: ${e}", ("e",msg));
      abort_receiver();
    }

    queue_mtx.lock();
    ((req_queue_entry*)data)->db_req_counter--;
    queue_mtx.unlock();
  }
}


class exp_chronos_plugin_impl : std::enable_shared_from_this<exp_chronos_plugin_impl> {
public:
  exp_chronos_plugin_impl():
    traces_io_context(),
    traces_worker(boost::asio::make_work_guard(traces_io_context)),
    fork_pause_timer(app().get_io_service())
  {}

  ~exp_chronos_plugin_impl() {
    traces_worker.reset();
    traces_thread_group.join_all();
  }


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
  const CassPrepared* prepared_ins_receipt_dates;
  const CassPrepared* prepared_ins_actions;
  const CassPrepared* prepared_ins_action_dates;
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
  chronicle::channels::receiver_pauses::channel_type::handle     _receiver_pauses_subscription;
  chronicle::channels::block_completed::channel_type::handle     _block_completed_subscription;

  const int channel_priority = 55;

  std::queue<std::shared_ptr<req_queue_entry>>        request_queue;
  boost::asio::io_context            traces_io_context;
  boost::asio::executor_work_guard<boost::asio::io_context::executor_type> traces_worker;
  size_t trace_treads = 1;
  boost::thread_group                traces_thread_group;

  boost::asio::deadline_timer        fork_pause_timer;

  uint32_t written_irreversible = 0;

  uint32_t trx_counter = 0;
  uint32_t block_counter = 0;
  boost::posix_time::ptime counter_start_time;

  // map keys are block dates currently in the processing queue
  boost::mutex date_maps_mtx;
  std::map<uint64_t, std::set<uint64_t>>                     receipt_date_written;
  std::map<uint64_t, std::map<uint64_t, std::set<uint64_t>>> action_date_written;

  void start() {
    std::atexit(atexit_handler);
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

    _receiver_pauses_subscription =
      app().get_channel<chronicle::channels::receiver_pauses>().subscribe
      ([this](std::shared_ptr<chronicle::channels::receiver_pause> rp){
        on_receiver_pause(rp);
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
    cass_cluster_set_request_timeout(cluster, 0);
    cass_cluster_set_num_threads_io(cluster, 4);
    cass_cluster_set_queue_size_io(cluster, 81920);
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
    check_future(future, "preparing ins_pointers");
    prepared_ins_pointers = cass_future_get_prepared(future);
    cass_future_free(future);


    future = cass_session_prepare
      (session, "INSERT INTO transactions (block_num, block_time, seq, trx_id, trace) VALUES (?,?,?,?,?)");
    check_future(future, "preparing ins_transactions");
    prepared_ins_transactions = cass_future_get_prepared(future);
    cass_future_free(future);


    future = cass_session_prepare
      (session,
       "INSERT INTO receipts (block_num, block_time, block_date, seq, account_name, recv_sequence_start, recv_sequence_count) VALUES (?,?,?,?,?,?,?)");
    check_future(future, "preparing ins_receipts");
    prepared_ins_receipts = cass_future_get_prepared(future);
    cass_future_free(future);


    future = cass_session_prepare
      (session,
       "INSERT INTO receipt_dates (account_name, block_date) VALUES (?,?)");
    check_future(future, "preparing ins_receipt_dates");
    prepared_ins_receipt_dates = cass_future_get_prepared(future);
    cass_future_free(future);


    future = cass_session_prepare
      (session, "INSERT INTO actions (block_num, block_time, block_date, seq, contract, action) VALUES (?,?,?,?,?,?)");
    check_future(future, "preparing ins_actions");
    prepared_ins_actions = cass_future_get_prepared(future);
    cass_future_free(future);


    future = cass_session_prepare
      (session,
       "INSERT INTO action_dates (contract, action, block_date) VALUES (?,?,?)");
    check_future(future, "preparing ins_action_dates");
    prepared_ins_action_dates = cass_future_get_prepared(future);
    cass_future_free(future);


    future = cass_session_prepare
      (session, "INSERT INTO abi_history (block_num, account_name, abi_raw) VALUES (?,?,?)");
    check_future(future, "preparing ins_abi_history");
    prepared_ins_abi_history = cass_future_get_prepared(future);
    cass_future_free(future);


    future = cass_session_prepare(session, "DELETE FROM transactions USING TIMEOUT 10s WHERE block_num=?");
    check_future(future, "preparing del_transactions");
    prepared_del_transactions = cass_future_get_prepared(future);
    cass_future_free(future);

    future = cass_session_prepare(session, "DELETE FROM receipts USING TIMEOUT 10s WHERE block_num=?");
    check_future(future, "preparing del_receipts");
    prepared_del_receipts = cass_future_get_prepared(future);
    cass_future_free(future);

    future = cass_session_prepare(session, "DELETE FROM actions USING TIMEOUT 10s WHERE block_num=?");
    check_future(future, "preparing del_actions");
    prepared_del_actions = cass_future_get_prepared(future);
    cass_future_free(future);

    future = cass_session_prepare(session, "DELETE FROM abi_history USING TIMEOUT 10s WHERE block_num=?");
    check_future(future, "preparing del_abi_history");
    prepared_del_abi_history = cass_future_get_prepared(future);
    cass_future_free(future);


    if( is_bootstrapping ) {
      ilog("Bootstrapping an empty database: writing all known ABI history");
      uint32_t count = 0;

      receiver_plug->walk_abi_history([&](uint64_t account, uint32_t block_index,
                                             const char* abi_data, size_t abi_size) {
        CassStatement* statement = cass_prepared_bind(prepared_ins_abi_history);
        size_t pos = 0;
        cass_statement_bind_int64(statement, pos++, block_index);
        cass_statement_bind_string(statement, pos++, eosio::name_to_string(account).c_str());
        cass_statement_bind_bytes(statement, pos++, (cass_byte_t*) abi_data, abi_size);
        future = cass_session_execute(session, statement);
        check_future(future, "inserting abi_history");
        cass_future_free(future);
        cass_statement_free(statement);
        ++count;
      });

      ilog("Wrote ${c} abi_history rows", ("c",count));
    }

    for(size_t i=0; i < trace_treads; ++i) {
      traces_thread_group.create_thread(boost::bind(&boost::asio::io_context::run, &traces_io_context));
    }

    counter_start_time = boost::posix_time::microsec_clock::local_time();
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
    queue_mtx.lock();
    while( !request_queue.empty() && request_queue.front()->is_finished() ) {
      request_queue.pop();
    }
    queue_mtx.unlock();

    size_t queue_size = request_queue.size();
    if( queue_size > 0 ) {
      ilog("ScyllaDB queue size is ${s}. Pausing the fork handling", ("s", queue_size));
      fork_pause_timer.expires_from_now(boost::posix_time::milliseconds(200));
      fork_pause_timer.async_wait(boost::bind(&exp_chronos_plugin_impl::on_fork, this, fe));
    }
    else {
      int64_t last_written_block = 0;

      CassStatement* statement = cass_statement_new("SELECT ptr FROM pointers WHERE id=0", 0);
      CassFuture* future = cass_session_execute(session, statement);
      check_future(future, "quering");
      const CassResult* result = cass_future_get_result(future);
      if( cass_result_row_count(result) > 0 ) {
        const CassRow* row = cass_result_first_row(result);
        const CassValue* column1 = cass_row_get_column(row, 0);
        cass_value_get_int64(column1,  &last_written_block);
      }
      cass_result_free(result);
      cass_statement_free(statement);
      cass_future_free(future);

      ilog("last_written_block: ${b}", ("b", last_written_block));

      if( last_written_block > 0 ) { // delete all written data down to the forked block
        ilog("Deleting data between blocks ${s} and ${e}", ("s", fe->block_num)("e", last_written_block));

        while( last_written_block >= fe->block_num ) {
          statement = cass_prepared_bind(prepared_del_transactions);
          cass_statement_bind_int64(statement, 0, last_written_block);
          future = cass_session_execute(session, statement);
          check_future(future, "deleting forked transactions");
          cass_future_free(future);
          cass_statement_free(statement);

          statement = cass_prepared_bind(prepared_del_receipts);
          cass_statement_bind_int64(statement, 0, last_written_block);
          future = cass_session_execute(session, statement);
          check_future(future, "deleting forked receipts");
          cass_future_free(future);
          cass_statement_free(statement);

          statement = cass_prepared_bind(prepared_del_actions);
          cass_statement_bind_int64(statement, 0, last_written_block);
          future = cass_session_execute(session, statement);
          check_future(future, "deleting forked actions");
          cass_future_free(future);
          cass_statement_free(statement);

          statement = cass_prepared_bind(prepared_del_abi_history);
          cass_statement_bind_int64(statement, 0, last_written_block);
          future = cass_session_execute(session, statement);
          check_future(future, "deleting forked abi_history");
          cass_future_free(future);
          cass_statement_free(statement);

          last_written_block--;
        }
      }

      ack_block(fe->block_num - 1);
    }
  }



  void on_block_started(std::shared_ptr<chronicle::channels::block_begins> bb) {
    uint64_t block_timestamp = bb->block_timestamp.to_time_point().elapsed.count() / 1000;
    uint64_t block_date = block_timestamp - (block_timestamp % MILLISECONDS_IN_A_DAY);

    auto tracker_ptr = make_shared<req_queue_entry>();
    tracker_ptr->block_num = bb->block_num;
    tracker_ptr->block_timestamp = block_timestamp;
    tracker_ptr->block_date = block_date;
    tracker_ptr->db_req_counter = 1;

    req_queue_entry* tracker = tracker_ptr.get();
    
    queue_mtx.lock();
    request_queue.push(std::move(tracker_ptr));
    queue_mtx.unlock();

    CassStatement* statement = cass_prepared_bind(prepared_ins_pointers);
    size_t pos = 0;
    cass_statement_bind_int32(statement, pos++, 0); // id=0: last written block
    cass_statement_bind_int64(statement, pos++, bb->block_num); // id=0: last written block

    CassFuture* future = cass_session_execute(session, statement);
    cass_future_set_callback(future, scylla_result_callback, tracker);
    cass_future_free(future);
    cass_statement_free(statement);
  }


  struct trace_job {
    std::shared_ptr<req_queue_entry>          tracker;
    uint64_t                                  global_seq = 0;
    std::map<uint64_t, uint64_t>              recv_seq_start;
    std::map<uint64_t, uint64_t>              recv_seq_max;
    std::map<uint64_t, std::set<uint64_t>>    actions_seen;
    std::array<uint8_t,32>                    trx_id;
    std::vector<uint8_t>                      raw_trace;
  };


  void on_transaction_trace(std::shared_ptr<chronicle::channels::transaction_trace> ccttr) {
    queue_mtx.lock();
    auto& tracker_ptr = request_queue.back();
    req_queue_entry* tracker = tracker_ptr.get();

    if( tracker->block_num != ccttr->block_num ) {
      elog("Trace block: ${b}, tracker: ${t}", ("b", ccttr->block_num)("t",tracker->block_num));
      throw std::runtime_error("tracker block is not the same as trace block");
    }

    tracker->trace_jobs_counter++;
    tracker->total_trx++;
    queue_mtx.unlock();

    auto job = make_shared<trace_job>();
    job->tracker = tracker_ptr;

    auto& trace = std::get<eosio::ship_protocol::transaction_trace_v0>(ccttr->trace);

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

      if( job->global_seq == 0 ) {
        job->global_seq = receipt->global_sequence;
      }

      if( job->recv_seq_start.count(receiver.value) == 0 ) {
        job->recv_seq_start.emplace(receiver.value, receipt->recv_sequence);
      }

      job->recv_seq_max.insert_or_assign(receiver.value, receipt->recv_sequence);

      eosio::name contract = act->account;
      if( receiver == contract ) {
        job->actions_seen[contract.value].insert(act->name.value);
      }
    }

    if( job->global_seq == 0 ) {
      throw std::runtime_error("global_seq is zero");
    }

    job->trx_id = trace.id.extract_as_byte_array();
    job->raw_trace.assign(ccttr->bin_start, ccttr->bin_start + ccttr->bin_size);

    ilog("PO block: ${b} seq: ${s}", ("b",tracker->block_num)("s",job->global_seq));
    traces_io_context.post(boost::bind(&exp_chronos_plugin_impl::process_transaction_trace, this, job));
  }


  void process_transaction_trace(std::shared_ptr<trace_job> job)
  {
    if( is_exiting ) {
      return;
    }

    req_queue_entry* tracker = job->tracker.get();
    uint32_t block_num = tracker->block_num;
    uint64_t block_timestamp = tracker->block_timestamp;
    uint64_t block_date = tracker->block_date;
    uint64_t global_seq = job->global_seq;

    ilog("EX block: ${b} seq: ${s}", ("b", block_num)("s",global_seq));
    
    {
      CassStatement* statement = cass_prepared_bind(prepared_ins_transactions);
      size_t pos = 0;
      cass_statement_bind_int64(statement, pos++, block_num);
      cass_statement_bind_int64(statement, pos++, block_timestamp);
      cass_statement_bind_int64(statement, pos++, global_seq);
      cass_statement_bind_bytes(statement, pos++, (cass_byte_t*)job->trx_id.data(), job->trx_id.size());
      cass_statement_bind_bytes(statement, pos++, (cass_byte_t*)job->raw_trace.data(), job->raw_trace.size());

      queue_mtx.lock();
      tracker->db_req_counter++;
      queue_mtx.unlock();

      CassFuture* future = cass_session_execute(session, statement);
      cass_future_set_callback(future, scylla_result_callback, tracker);
      cass_future_free(future);
      cass_statement_free(statement);
    }

    for(auto item: job->recv_seq_start) {
      {
        CassStatement* statement = cass_prepared_bind(prepared_ins_receipts);
        size_t pos = 0;
        cass_statement_bind_int64(statement, pos++, block_num);
        cass_statement_bind_int64(statement, pos++, block_timestamp);
        cass_statement_bind_int64(statement, pos++, block_date);
        cass_statement_bind_int64(statement, pos++, global_seq);
        cass_statement_bind_string(statement, pos++, eosio::name_to_string(item.first).c_str());
        cass_statement_bind_int64(statement, pos++, item.second);
        cass_statement_bind_int64(statement, pos++, job->recv_seq_max.at(item.first) - item.second + 1);

        queue_mtx.lock();
        tracker->db_req_counter++;
        queue_mtx.unlock();

        CassFuture* future = cass_session_execute(session, statement);
        cass_future_set_callback(future, scylla_result_callback, tracker);
        cass_future_free(future);
        cass_statement_free(statement);
      }

      date_maps_mtx.lock();
      if( receipt_date_written[block_date].count(item.first) == 0 )
      {
        receipt_date_written[block_date].insert(item.first);
        date_maps_mtx.unlock();

        CassStatement* statement = cass_prepared_bind(prepared_ins_receipt_dates);
        size_t pos = 0;
        cass_statement_bind_string(statement, pos++, eosio::name_to_string(item.first).c_str());
        cass_statement_bind_int64(statement, pos++, block_date);

        queue_mtx.lock();
        tracker->db_req_counter++;
        queue_mtx.unlock();

        CassFuture* future = cass_session_execute(session, statement);
        cass_future_set_callback(future, scylla_result_callback, tracker);
        cass_future_free(future);
        cass_statement_free(statement);
      }
      else {
        date_maps_mtx.unlock();
      }
    }

    for(auto item: job->actions_seen) {
      for(auto aname: item.second) {
        {
          CassStatement* statement = cass_prepared_bind(prepared_ins_actions);
          size_t pos = 0;
          cass_statement_bind_int64(statement, pos++, block_num);
          cass_statement_bind_int64(statement, pos++, block_timestamp);
          cass_statement_bind_int64(statement, pos++, block_date);
          cass_statement_bind_int64(statement, pos++, global_seq);
          cass_statement_bind_string(statement, pos++, eosio::name_to_string(item.first).c_str());
          cass_statement_bind_string(statement, pos++, eosio::name_to_string(aname).c_str());

          queue_mtx.lock();
          tracker->db_req_counter++;
          queue_mtx.unlock();

          CassFuture* future = cass_session_execute(session, statement);
          cass_future_set_callback(future, scylla_result_callback, tracker);
          cass_future_free(future);
          cass_statement_free(statement);
        }

        date_maps_mtx.lock();
        if( action_date_written[block_date][item.first].count(aname) == 0 )
        {
          action_date_written[block_date][item.first].insert(aname);
          date_maps_mtx.unlock();

          CassStatement* statement = cass_prepared_bind(prepared_ins_action_dates);
          size_t pos = 0;
          cass_statement_bind_string(statement, pos++, eosio::name_to_string(item.first).c_str());
          cass_statement_bind_string(statement, pos++, eosio::name_to_string(aname).c_str());
          cass_statement_bind_int64(statement, pos++, block_date);

          queue_mtx.lock();
          tracker->db_req_counter++;
          queue_mtx.unlock();

          CassFuture* future = cass_session_execute(session, statement);
          cass_future_set_callback(future, scylla_result_callback, tracker);
          cass_future_free(future);
          cass_statement_free(statement);
        }
        else {
          date_maps_mtx.unlock();
        }
      }
    }

    queue_mtx.lock();
    tracker->trace_jobs_counter--;
    queue_mtx.unlock();
  }


  void on_abi_update(std::shared_ptr<chronicle::channels::abi_update> abiupd) {
    CassStatement* statement = cass_prepared_bind(prepared_ins_abi_history);
    size_t pos = 0;
    cass_statement_bind_int64(statement, pos++, abiupd->block_num);
    cass_statement_bind_string(statement, pos++, eosio::name_to_string(abiupd->account.value).c_str());
    cass_statement_bind_bytes(statement, pos++, (cass_byte_t*) abiupd->bin_start, abiupd->bin_size);
    CassFuture* future = cass_session_execute(session, statement);
    check_future(future, "inserting abi_history");
    cass_future_free(future);
    cass_statement_free(statement);
  }


  void on_abi_removal(std::shared_ptr<chronicle::channels::abi_removal> ar) {
    CassStatement* statement = cass_prepared_bind(prepared_ins_abi_history);
    size_t pos = 0;
    cass_statement_bind_int64(statement, pos++, ar->block_num);
    cass_statement_bind_string(statement, pos++, eosio::name_to_string(ar->account.value).c_str());
    string empty;
    cass_statement_bind_bytes(statement, pos++, (cass_byte_t*) empty.data(), 0);
    CassFuture* future = cass_session_execute(session, statement);
    check_future(future, "inserting empty abi_history");
    cass_future_free(future);
    cass_statement_free(statement);
  }


  void on_receiver_pause(std::shared_ptr<chronicle::channels::receiver_pause> rp) {
    ack_finished_blocks();
  }



  void on_block_completed(std::shared_ptr<block_finished> bf) {
    req_queue_entry* tracker = request_queue.back().get();

    if( is_bootstrapping ) {
      CassStatement* statement = cass_prepared_bind(prepared_ins_pointers);
      size_t pos = 0;
      cass_statement_bind_int32(statement, pos++, 2); // id=2: lowest block in history
      cass_statement_bind_int64(statement, pos++, bf->block_num);

      queue_mtx.lock();
      tracker->db_req_counter++;
      queue_mtx.unlock();

      CassFuture* future = cass_session_execute(session, statement);
      cass_future_set_callback(future, scylla_result_callback, tracker);
      cass_future_free(future);
      cass_statement_free(statement);

      is_bootstrapping = false;
    }

    if( bf->last_irreversible > written_irreversible ) {
      CassStatement* statement = cass_prepared_bind(prepared_ins_pointers);
      size_t pos = 0;
      cass_statement_bind_int32(statement, pos++, 1); // id=1: irreversible block,
      cass_statement_bind_int64(statement, pos++, bf->block_num);

      queue_mtx.lock();
      tracker->db_req_counter++;
      queue_mtx.unlock();

      CassFuture* future = cass_session_execute(session, statement);
      cass_future_set_callback(future, scylla_result_callback, tracker);
      cass_future_free(future);
      cass_statement_free(statement);

      written_irreversible = bf->last_irreversible;
    }

    ack_finished_blocks();
  }



  void ack_finished_blocks() {
    uint32_t ack = 0;
    uint64_t date_max = 0;
    queue_mtx.lock();
    {
      auto& tracker = request_queue.front();
      ilog("block=${b} trace_jobs_counter = ${t}  db_req_counter = ${d} total_trx = ${x}",
           ("b",tracker->block_num)("t",tracker->trace_jobs_counter)("d",tracker->db_req_counter)("x",tracker->total_trx));
    }
    while( !request_queue.empty() && request_queue.front()->is_finished() ) {
      req_queue_entry* tracker = request_queue.front().get();
      ack = tracker->block_num;
      trx_counter += tracker->total_trx;
      if( date_max < tracker->block_date ) {
        date_max = tracker->block_date;
      }
      request_queue.pop();
      block_counter++;
    }
    queue_mtx.unlock();

    // clean up old entries in the maps
    date_maps_mtx.lock();
    auto recepts_iter = receipt_date_written.begin();
    while( recepts_iter->first < date_max ) {
      recepts_iter = receipt_date_written.erase(recepts_iter);
    }

    auto actions_iter = action_date_written.begin();
    while( actions_iter->first < date_max ) {
      actions_iter = action_date_written.erase(actions_iter);
    }
    date_maps_mtx.unlock();

    if( ack > 0 ) {
      ack_block(ack);
      ilog("a ${a}", ("a", ack));

      if( block_counter >= 200 ) {
        boost::posix_time::ptime now = boost::posix_time::microsec_clock::local_time();
        boost::posix_time::time_duration diff = now - counter_start_time;
        uint32_t millisecs = diff.total_milliseconds();

        if( millisecs > 0 ) {
          ilog("ack ${a}, blocks/s: ${b}, trx/s: ${t}",
               ("a", ack)("b", 1000*block_counter/millisecs)("t", 1000*trx_counter/millisecs));

          counter_start_time = now;
          block_counter = 0;
          trx_counter = 0;
        }
      }
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
