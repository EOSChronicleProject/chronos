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
  const char* SCYLLA_IO_THREADS_OPT = "scylla-io-threads";
  const char* SCYLLA_CONNS_PER_HOST_OPT = "scylla-connections-per-host";
  const char* SCYLLA_USERNAME_OPT = "scylla-username";
  const char* SCYLLA_PASSWORD_OPT = "scylla-password";
  const char* SCYLLA_CONSISTENCY_OPT = "scylla-consistency";

  const char* CHRONOS_MAXUNACK_OPT = "chronos-max-unack";
  const char* CHRONOS_TRACE_THREADS_OPT = "chronos-trace-threads";

  const uint64_t MILLISECONDS_IN_A_DAY = 24 * 3600 * 1000;
}

// scylla client threads may exit after main thread, so we we need to organize a clean aborting

static std::mutex track_mtx;
static std::mutex track_jobs_counter_mtx;
static bool is_exiting = false;
static uint32_t global_db_req_counter = 0;

void atexit_handler()
{
  is_exiting = true;
}

// tracker for ScyllaDB asynchronous requests
struct block_track_entry {
  uint32_t block_num;
  uint64_t block_timestamp;
  uint64_t block_date;
  uint32_t trace_jobs_counter = 0;
  uint32_t db_req_counter = 0;
  uint32_t total_trx = 0;
  bool     block_complete = false;

  inline bool is_finished() const {return (block_complete && (trace_jobs_counter + db_req_counter == 0));}
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
      return;
    }

    std::unique_lock<std::mutex> lock(track_mtx);
    block_track_entry* tracker = (block_track_entry*)data;

    if( tracker->db_req_counter == 0 ) {
      elog("tracker underflow, block= ${b}", ("b",tracker->block_num));
      abort_receiver();
    }
    tracker->db_req_counter--;
    global_db_req_counter++;
  }
}

struct trace_job {
  std::shared_ptr<block_track_entry>                      tracker;
  std::shared_ptr<chronicle::channels::transaction_trace> ccttr;
};




class exp_chronos_plugin_impl : std::enable_shared_from_this<exp_chronos_plugin_impl> {
public:
  exp_chronos_plugin_impl():
    traces_worker(boost::asio::make_work_guard(traces_io_context)),
    fork_pause_timer(app().get_io_service())
  {}

  ~exp_chronos_plugin_impl() {
  }


  string scylla_hosts;
  uint16_t scylla_port;
  string scylla_keyspace;
  uint16_t scylla_io_threads;
  uint16_t scylla_conn_per_host;
  string scylla_username;
  string scylla_password;
  CassConsistency scylla_consistency;

  uint32_t maxunack;

  CassCluster* cluster;
  CassSession* session;

  bool is_bootstrapping = false;

  const CassPrepared* prepared_ins_pointers;
  const CassPrepared* prepared_ins_blocks;
  const CassPrepared* prepared_ins_transactions;
  const CassPrepared* prepared_ins_receipts;
  const CassPrepared* prepared_ins_receipt_dates;
  const CassPrepared* prepared_ins_actions;
  const CassPrepared* prepared_ins_action_dates;
  const CassPrepared* prepared_ins_abi_history;

  const CassPrepared* prepared_del_blocks;
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

  std::queue<std::shared_ptr<block_track_entry>>        block_track_queue;

  size_t                             trace_treads;
  boost::thread_group                traces_thread_group;
  boost::asio::io_context            traces_io_context;
  boost::asio::executor_work_guard<boost::asio::io_context::executor_type> traces_worker;

  boost::asio::deadline_timer        fork_pause_timer;

  uint32_t written_irreversible = 0;

  uint32_t trx_counter = 0;
  uint32_t block_counter = 0;
  boost::posix_time::ptime counter_start_time;

  // map keys are block dates currently in the processing queue
  std::mutex date_maps_mtx;
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
    cass_cluster_set_request_timeout(cluster, 100000);
    cass_cluster_set_num_threads_io(cluster, scylla_io_threads);
    cass_cluster_set_queue_size_io(cluster, 1048576);
    if( scylla_username.size() > 0 ) {
      cass_cluster_set_credentials(cluster, scylla_username.c_str(), scylla_password.c_str());
    }
    cass_cluster_set_consistency(cluster, scylla_consistency);

    CassFuture* future;

    ilog("Connecting to ScyllaDB cluster: ${h}", ("h",scylla_hosts));
    future = cass_session_connect_keyspace(session, cluster, scylla_keyspace.c_str());
    check_future(future, "connecting to ScyllaDB cluster");
    cass_future_free(future);

    {
      CassStatement* statement = cass_statement_new("SELECT ptr FROM pointers WHERE id=2", 0); // id=2: lowest block in history
      future = cass_session_execute(session, statement);
      check_future(future, "quering pointers");
      const CassResult* result = cass_future_get_result(future);
      if( cass_result_row_count(result) == 0 ) {
        is_bootstrapping = true;
      }
      cass_result_free(result);
      cass_statement_free(statement);
      cass_future_free(future);
    }

    {
      CassStatement* statement = cass_statement_new("SELECT ptr FROM pointers WHERE id=1", 0); // id=1: irreversible block,
      future = cass_session_execute(session, statement);
      check_future(future, "quering pointers");
      const CassResult* result = cass_future_get_result(future);
      if( cass_result_row_count(result) > 0 ) {
        const CassRow* row = cass_result_first_row(result);
        const CassValue* column1 = cass_row_get_column(row, 0);
        int64_t val;
        cass_value_get_int64(column1,  &val);
        written_irreversible = (uint32_t) val;
        ilog("Irreversible block from the database: ${b}", ("b", written_irreversible));
      }
      cass_result_free(result);
      cass_statement_free(statement);
      cass_future_free(future);
    }


    future = cass_session_prepare
      (session, "INSERT INTO pointers (id, ptr) VALUES (?,?) USING TIMEOUT 100s");
    check_future(future, "preparing ins_pointers");
    prepared_ins_pointers = cass_future_get_prepared(future);
    cass_future_free(future);


    future = cass_session_prepare
      (session, "INSERT INTO blocks (block_num, block_time, block_date, block_id, producer, previous, transaction_mroot, action_mroot, trx_count) VALUES (?,?,?,?,?,?,?,?,?) USING TIMEOUT 100s");
    check_future(future, "preparing ins_blocks");
    prepared_ins_blocks = cass_future_get_prepared(future);
    cass_future_free(future);


    future = cass_session_prepare
      (session, "INSERT INTO transactions (block_num, block_time, seq, trx_id, trace) VALUES (?,?,?,?,?) USING TIMEOUT 100s");
    check_future(future, "preparing ins_transactions");
    prepared_ins_transactions = cass_future_get_prepared(future);
    cass_future_free(future);


    future = cass_session_prepare
      (session,
       "INSERT INTO receipts (block_num, block_time, block_date, seq, account_name, recv_sequence_start, recv_sequence_count) VALUES (?,?,?,?,?,?,?) USING TIMEOUT 100s");
    check_future(future, "preparing ins_receipts");
    prepared_ins_receipts = cass_future_get_prepared(future);
    cass_future_free(future);


    future = cass_session_prepare
      (session,
       "INSERT INTO receipt_dates (account_name, block_date) VALUES (?,?) USING TIMEOUT 100s");
    check_future(future, "preparing ins_receipt_dates");
    prepared_ins_receipt_dates = cass_future_get_prepared(future);
    cass_future_free(future);


    future = cass_session_prepare
      (session, "INSERT INTO actions (block_num, block_time, block_date, seq, contract, action) VALUES (?,?,?,?,?,?) USING TIMEOUT 100s");
    check_future(future, "preparing ins_actions");
    prepared_ins_actions = cass_future_get_prepared(future);
    cass_future_free(future);


    future = cass_session_prepare
      (session,
       "INSERT INTO action_dates (contract, action, block_date) VALUES (?,?,?) USING TIMEOUT 100s");
    check_future(future, "preparing ins_action_dates");
    prepared_ins_action_dates = cass_future_get_prepared(future);
    cass_future_free(future);


    future = cass_session_prepare
      (session, "INSERT INTO abi_history (block_num, account_name, abi_raw) VALUES (?,?,?) USING TIMEOUT 100s");
    check_future(future, "preparing ins_abi_history");
    prepared_ins_abi_history = cass_future_get_prepared(future);
    cass_future_free(future);


    future = cass_session_prepare(session, "DELETE FROM blocks USING TIMEOUT 100s WHERE block_num=?");
    check_future(future, "preparing del_blocks");
    prepared_del_blocks = cass_future_get_prepared(future);
    cass_future_free(future);

    future = cass_session_prepare(session, "DELETE FROM transactions USING TIMEOUT 100s WHERE block_num=?");
    check_future(future, "preparing del_transactions");
    prepared_del_transactions = cass_future_get_prepared(future);
    cass_future_free(future);

    future = cass_session_prepare(session, "DELETE FROM receipts USING TIMEOUT 100s WHERE block_num=?");
    check_future(future, "preparing del_receipts");
    prepared_del_receipts = cass_future_get_prepared(future);
    cass_future_free(future);

    future = cass_session_prepare(session, "DELETE FROM actions USING TIMEOUT 100s WHERE block_num=?");
    check_future(future, "preparing del_actions");
    prepared_del_actions = cass_future_get_prepared(future);
    cass_future_free(future);

    future = cass_session_prepare(session, "DELETE FROM abi_history USING TIMEOUT 100s WHERE block_num=?");
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

  void stop() {
    is_exiting = true;
    traces_io_context.stop();
    traces_thread_group.join_all();

    CassFuture* future = cass_session_close(session);
    check_future(future, "closing the session");
    ilog("Disconnected from ScyllaDB cluster");
    cass_future_free(future);
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
    ilog("fork: ${f}", ("f", fe->block_num));

    {
      std::unique_lock<std::mutex> lock(track_mtx);
      while( !block_track_queue.empty() && block_track_queue.front()->is_finished() ) {
        block_track_queue.pop();
      }
    }

    size_t queue_size = block_track_queue.size();
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

        while( last_written_block >= fe->block_num && last_written_block > written_irreversible ) {
          statement = cass_prepared_bind(prepared_del_blocks);
          cass_statement_bind_int64(statement, 0, last_written_block);
          future = cass_session_execute(session, statement);
          check_future(future, "deleting forked blocks");
          cass_future_free(future);
          cass_statement_free(statement);

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

    auto tracker_ptr = make_shared<block_track_entry>();
    tracker_ptr->block_num = bb->block_num;
    tracker_ptr->block_timestamp = block_timestamp;
    tracker_ptr->block_date = block_date;
    block_track_queue.push(tracker_ptr);

    if( bb->block_num > written_irreversible ) {
      tracker_ptr->db_req_counter = 1;

      block_track_entry* tracker = tracker_ptr.get();

      CassStatement* statement = cass_prepared_bind(prepared_ins_pointers);
      size_t pos = 0;
      cass_statement_bind_int32(statement, pos++, 0); // id=0: last written block
      cass_statement_bind_int64(statement, pos++, bb->block_num);

      CassFuture* future = cass_session_execute(session, statement);
      cass_future_set_callback(future, scylla_result_callback, tracker);
      cass_future_free(future);
      cass_statement_free(statement);
    }
  }


  void on_transaction_trace(std::shared_ptr<chronicle::channels::transaction_trace> ccttr) {
    if( ccttr->block_num <= written_irreversible ) {
      return;
    }

    auto& trace = std::get<eosio::ship_protocol::transaction_trace_v0>(ccttr->trace);
    if( trace.status != eosio::ship_protocol::transaction_status::executed ) {
      return;
    }

    std::shared_ptr<block_track_entry>& tracker_ptr = block_track_queue.back();
    block_track_entry* tracker = tracker_ptr.get();

    if( tracker->block_num != ccttr->block_num ) {
      elog("Trace block: ${b}, tracker: ${t}", ("b", ccttr->block_num)("t",tracker->block_num));
      throw std::runtime_error("tracker block is not the same as trace block");
    }

    {
      std::unique_lock<std::mutex> lock(track_jobs_counter_mtx);
      tracker->trace_jobs_counter++;
      tracker->total_trx++;
    }

    auto job = make_shared<trace_job>();
    trace_job* job_entry = job.get();
    job_entry->tracker = tracker_ptr;
    job_entry->ccttr = ccttr;

    traces_io_context.post(boost::bind(&exp_chronos_plugin_impl::process_transaction_trace, this, job));
  }


  void process_transaction_trace(std::shared_ptr<trace_job> job)
  {
    if( is_exiting ) {
      return;
    }

    auto& trace = std::get<eosio::ship_protocol::transaction_trace_v0>(job->ccttr->trace);

    uint64_t                                  global_seq = 0;
    std::map<uint64_t, uint64_t>              recv_seq_start;
    std::map<uint64_t, uint64_t>              recv_seq_max;
    std::map<uint64_t, std::set<uint64_t>>    actions_seen;

    for( auto& atrace: trace.action_traces ) {

      eosio::ship_protocol::action*               act;
      eosio::ship_protocol::action_receipt_v0*    receipt;
      eosio::name                                 receiver;

      size_t index = atrace.index();
      if( index == 0 ) {
        eosio::ship_protocol::action_trace_v0& at = std::get<eosio::ship_protocol::action_trace_v0>(atrace);
        if( ! at.receipt ) {
          break;
        }
        act = &at.act;
        receipt = &(std::get<eosio::ship_protocol::action_receipt_v0>(at.receipt.value()));
        receiver = at.receiver;
      }
      else if( index == 1 ) {
        eosio::ship_protocol::action_trace_v1& at = std::get<eosio::ship_protocol::action_trace_v1>(atrace);
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
        recv_seq_start.insert_or_assign(receiver.value, receipt->recv_sequence);
      }

      recv_seq_max.insert_or_assign(receiver.value, receipt->recv_sequence);

      if( receiver == act->account ) {
        actions_seen[act->account.value].insert(act->name.value);
      }
    }

    if( global_seq == 0 ) {
      throw std::runtime_error("global_seq is zero");
    }

    const std::array<uint8_t,32>& trx_id = trace.id.extract_as_byte_array();

    block_track_entry* tracker = job->tracker.get();
    uint32_t block_num = tracker->block_num;
    uint64_t block_timestamp = tracker->block_timestamp;
    uint64_t block_date = tracker->block_date;

    {
      CassStatement* statement = cass_prepared_bind(prepared_ins_transactions);
      size_t pos = 0;
      cass_statement_bind_int64(statement, pos++, block_num);
      cass_statement_bind_int64(statement, pos++, block_timestamp);
      cass_statement_bind_int64(statement, pos++, global_seq);
      cass_statement_bind_bytes(statement, pos++, (cass_byte_t*)trx_id.data(), trx_id.size());
      cass_statement_bind_bytes(statement, pos++, (cass_byte_t*)job->ccttr->bin_start, job->ccttr->bin_size);

      {
        std::unique_lock<std::mutex> lock(track_mtx);
        tracker->db_req_counter++;
      }

      CassFuture* future = cass_session_execute(session, statement);
      cass_future_set_callback(future, scylla_result_callback, tracker);
      cass_future_free(future);
      cass_statement_free(statement);
    }

    for(auto item: recv_seq_start) {
      {
        CassStatement* statement = cass_prepared_bind(prepared_ins_receipts);
        size_t pos = 0;
        cass_statement_bind_int64(statement, pos++, block_num);
        cass_statement_bind_int64(statement, pos++, block_timestamp);
        cass_statement_bind_int64(statement, pos++, block_date);
        cass_statement_bind_int64(statement, pos++, global_seq);
        cass_statement_bind_string(statement, pos++, eosio::name_to_string(item.first).c_str());
        cass_statement_bind_int64(statement, pos++, item.second);
        cass_statement_bind_int64(statement, pos++, recv_seq_max.at(item.first) - item.second + 1);

        {
          std::unique_lock<std::mutex> lock(track_mtx);
          tracker->db_req_counter++;
        }

        CassFuture* future = cass_session_execute(session, statement);
        cass_future_set_callback(future, scylla_result_callback, tracker);
        cass_future_free(future);
        cass_statement_free(statement);
      }

      std::unique_lock<std::mutex> lock(date_maps_mtx);
      if( receipt_date_written[block_date].count(item.first) == 0 )
      {
        receipt_date_written[block_date].insert(item.first);

        CassStatement* statement = cass_prepared_bind(prepared_ins_receipt_dates);
        size_t pos = 0;
        cass_statement_bind_string(statement, pos++, eosio::name_to_string(item.first).c_str());
        cass_statement_bind_int64(statement, pos++, block_date);

        {
          std::unique_lock<std::mutex> lock(track_mtx);
          tracker->db_req_counter++;
        }

        CassFuture* future = cass_session_execute(session, statement);
        cass_future_set_callback(future, scylla_result_callback, tracker);
        cass_future_free(future);
        cass_statement_free(statement);
      }
    }

    for(auto item: actions_seen) {
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

          {
            std::unique_lock<std::mutex> lock(track_mtx);
            tracker->db_req_counter++;
          }

          CassFuture* future = cass_session_execute(session, statement);
          cass_future_set_callback(future, scylla_result_callback, tracker);
          cass_future_free(future);
          cass_statement_free(statement);
        }

        std::unique_lock<std::mutex> lock(date_maps_mtx);
        if( action_date_written[block_date][item.first].count(aname) == 0 )
        {
          action_date_written[block_date][item.first].insert(aname);

          CassStatement* statement = cass_prepared_bind(prepared_ins_action_dates);
          size_t pos = 0;
          cass_statement_bind_string(statement, pos++, eosio::name_to_string(item.first).c_str());
          cass_statement_bind_string(statement, pos++, eosio::name_to_string(aname).c_str());
          cass_statement_bind_int64(statement, pos++, block_date);

          {
            std::unique_lock<std::mutex> lock(track_mtx);
            tracker->db_req_counter++;
          }

          CassFuture* future = cass_session_execute(session, statement);
          cass_future_set_callback(future, scylla_result_callback, tracker);
          cass_future_free(future);
          cass_statement_free(statement);
        }
      }
    }

    std::unique_lock<std::mutex> lock(track_jobs_counter_mtx);
    tracker->trace_jobs_counter--;
  }


  void on_abi_update(std::shared_ptr<chronicle::channels::abi_update> abiupd) {
    CassStatement* statement = cass_prepared_bind(prepared_ins_abi_history);
    size_t pos = 0;
    cass_statement_bind_int64(statement, pos++, abiupd->block_num);
    cass_statement_bind_string(statement, pos++, eosio::name_to_string(abiupd->account.value).c_str());
    cass_statement_bind_bytes(statement, pos++, (cass_byte_t*) abiupd->binary.data(), abiupd->binary.size());
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
    if( block_track_queue.empty() ) {
      throw std::runtime_error("block_track_queue is empty in on_block_completed");
    }

    block_track_entry* tracker = block_track_queue.back().get();

    {
      const std::array<uint8_t,32>& block_id = bf->block_id.extract_as_byte_array();
      const std::array<uint8_t,32>& previous = bf->previous.extract_as_byte_array();
      const std::array<uint8_t,32>& transaction_mroot = bf->transaction_mroot.extract_as_byte_array();
      const std::array<uint8_t,32>& action_mroot = bf->action_mroot.extract_as_byte_array();

      CassStatement* statement = cass_prepared_bind(prepared_ins_blocks);
      size_t pos = 0;
      cass_statement_bind_int64(statement, pos++, bf->block_num);
      cass_statement_bind_int64(statement, pos++, tracker->block_timestamp);
      cass_statement_bind_int64(statement, pos++, tracker->block_date);
      cass_statement_bind_bytes(statement, pos++, (cass_byte_t*)block_id.data(), block_id.size());
      cass_statement_bind_string(statement, pos++, eosio::name_to_string(bf->producer.value).c_str());
      cass_statement_bind_bytes(statement, pos++, (cass_byte_t*)previous.data(), previous.size());
      cass_statement_bind_bytes(statement, pos++, (cass_byte_t*)transaction_mroot.data(), transaction_mroot.size());
      cass_statement_bind_bytes(statement, pos++, (cass_byte_t*)action_mroot.data(), action_mroot.size());
      cass_statement_bind_int32(statement, pos++, bf->trx_count);

      {
        std::unique_lock<std::mutex> lock(track_mtx);
        tracker->db_req_counter++;
        tracker->block_complete = true;
      }

      CassFuture* future = cass_session_execute(session, statement);
      cass_future_set_callback(future, scylla_result_callback, tracker);
      cass_future_free(future);
      cass_statement_free(statement);
    }

    if( is_bootstrapping ) {
      CassStatement* statement = cass_prepared_bind(prepared_ins_pointers);
      size_t pos = 0;
      cass_statement_bind_int32(statement, pos++, 2); // id=2: lowest block in history
      cass_statement_bind_int64(statement, pos++, bf->block_num);

      {
        std::unique_lock<std::mutex> lock(track_mtx);
        tracker->db_req_counter++;
      }

      CassFuture* future = cass_session_execute(session, statement);
      cass_future_set_callback(future, scylla_result_callback, tracker);
      cass_future_free(future);
      cass_statement_free(statement);

      is_bootstrapping = false;
    }

    if( bf->block_num > written_irreversible ) {
      if( bf->last_irreversible > written_irreversible ) {
        uint32_t old_written_irreversible = written_irreversible;
        if( bf->last_irreversible > bf->block_num ) {
          // last irreversible is in the future, we are catching up through the old history
          written_irreversible = bf->block_num;
        }
        else {
          // we are near the head block
          written_irreversible = bf->last_irreversible;
        }

        if( written_irreversible > old_written_irreversible ) {
          CassStatement* statement = cass_prepared_bind(prepared_ins_pointers);
          size_t pos = 0;
          cass_statement_bind_int32(statement, pos++, 1); // id=1: irreversible block,
          cass_statement_bind_int64(statement, pos++, written_irreversible);

          {
            std::unique_lock<std::mutex> lock(track_mtx);
            tracker->db_req_counter++;
          }

          CassFuture* future = cass_session_execute(session, statement);
          cass_future_set_callback(future, scylla_result_callback, tracker);
          cass_future_free(future);
          cass_statement_free(statement);
        }
      }
    }

    ack_finished_blocks();
  }



  void ack_finished_blocks() {
    uint32_t ack = 0;
    uint64_t date_max = 0;

    while( !block_track_queue.empty() && block_track_queue.front()->is_finished() ) {
      block_track_entry* tracker = block_track_queue.front().get();
      ack = tracker->block_num;
      trx_counter += tracker->total_trx;
      if( date_max < tracker->block_date ) {
        date_max = tracker->block_date;
      }
      block_track_queue.pop();
      block_counter++;
    }

    if( ack > 0 ) {
      ack_block(ack);

      boost::posix_time::ptime now = boost::posix_time::microsec_clock::local_time();
      boost::posix_time::time_duration diff = now - counter_start_time;
      uint32_t millisecs = diff.total_milliseconds();

      if( millisecs > 5000 ) {
        ilog("ack ${a}, blocks/s: ${b}, trx/s: ${t}, queue: ${q}, req/s: ${r}",
             ("a", ack)("b", 1000*block_counter/millisecs)("t", 1000*trx_counter/millisecs)
             ("q", block_track_queue.size())
             ("r", 1000*global_db_req_counter/millisecs));

        counter_start_time = now;
        block_counter = 0;
        trx_counter = 0;
        {
          std::unique_lock<std::mutex> lock(track_mtx);
          global_db_req_counter = 0;
        }

        // clean up old entries in the maps
        {
          std::unique_lock<std::mutex> lock(date_maps_mtx);
          auto recepts_iter = receipt_date_written.begin();
          while( recepts_iter != receipt_date_written.end() && recepts_iter->first < date_max ) {
            recepts_iter = receipt_date_written.erase(recepts_iter);
          }

          auto actions_iter = action_date_written.begin();
          while( actions_iter != action_date_written.end() && actions_iter->first < date_max ) {
            actions_iter = action_date_written.erase(actions_iter);
          }
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
    (SCYLLA_IO_THREADS_OPT, bpo::value<uint16_t>()->default_value(4), "Number of Scylla client I/O threads")
    (SCYLLA_CONNS_PER_HOST_OPT, bpo::value<uint16_t>()->default_value(1), "Number of Scylla client connections per host")
    (SCYLLA_USERNAME_OPT, bpo::value<string>(), "ScyllaDB authentication name")
    (SCYLLA_PASSWORD_OPT, bpo::value<string>(), "ScyllaDB authentication password")
    (SCYLLA_CONSISTENCY_OPT, bpo::value<uint16_t>()->default_value(CASS_CONSISTENCY_QUORUM), "Cluster consistency level")
    (CHRONOS_MAXUNACK_OPT, bpo::value<uint32_t>()->default_value(1000),
     "Receiver will pause at so many unacknowledged blocks")
    (CHRONOS_TRACE_THREADS_OPT, bpo::value<uint16_t>()->default_value(4), "Number of trace processing threads")
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
    my->scylla_io_threads = options.at(SCYLLA_IO_THREADS_OPT).as<uint16_t>();
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

    my->scylla_consistency = (CassConsistency) options.at(SCYLLA_CONSISTENCY_OPT).as<uint16_t>();

    my->maxunack = options.at(CHRONOS_MAXUNACK_OPT).as<uint32_t>();
    if( my->maxunack == 0 )
      throw std::runtime_error("Maximum unacked blocks must be a positive integer");

    my->trace_treads = options.at(CHRONOS_TRACE_THREADS_OPT).as<uint16_t>();
    if( my->trace_treads == 0 )
      throw std::runtime_error("Maximum threads must be a positive integer");

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
    my->stop();
    ilog("exp_chronos_plugin stopped");
  }
}
