// copyright defined in LICENSE.txt

#include "exp_chronos_plugin.hpp"
#include "receiver_plugin.hpp"

#include <iostream>
#include <string>
#include <sstream>

#include <fc/log/logger.hpp>
#include <fc/exception/exception.hpp>

using namespace chronicle::channels;
using namespace abieos;

using std::make_shared;
using std::string;

static auto _exp_chronos_plugin = app().register_plugin<exp_chronos_plugin>();

namespace {
  const char* SCYLLA_HOST_OPT = "scylla-host";
}


class exp_chronos_plugin_impl : std::enable_shared_from_this<exp_chronos_plugin_impl> {
public:
  exp_chronos_plugin_impl()
  {}

  chronicle::channels::forks::channel_type::handle               _forks_subscription;
  chronicle::channels::transaction_traces::channel_type::handle  _transaction_traces_subscription;
  chronicle::channels::abi_removals::channel_type::handle        _abi_removals_subscription;
  chronicle::channels::abi_updates::channel_type::handle         _abi_updates_subscription;
  chronicle::channels::block_completed::channel_type::handle     _block_completed_subscription;

  const int channel_priority = 55;


  void start() {
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
    (SCYLLA_HOST_OPT, bpo::value<string>(), "ScyllaDB server host");
}


void exp_chronos_plugin::plugin_initialize( const variables_map& options ) {
  if (is_noexport_opt(options))
    return;
  try {
    donot_start_receiver_before(this, "exp_chronos_plugin");
    ilog("Initialized exp_chronos_plugin");
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
