#include <appbase/application.hpp>

using namespace appbase;


class exp_chronos_plugin : public appbase::plugin<exp_chronos_plugin>
{
public:
  APPBASE_PLUGIN_REQUIRES();
  exp_chronos_plugin();
  virtual ~exp_chronos_plugin();
  virtual void set_program_options(options_description& cli, options_description& cfg) override;  
  void plugin_initialize(const variables_map& options);
  void plugin_startup();
  void plugin_shutdown();
  
private:
  std::unique_ptr<class exp_chronos_plugin_impl> my;
};



