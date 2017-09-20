require 'masker/configuration'
require 'masker/adapters/postgres'

class Masker
#opts = {
  #no_mask: {
    #table1: {
      #ids: []
    #},
    #table2: {
      #ids: []
    #}
  #}
#}
     #database_url:
     #mask:
       #table1:
         #column1: name
         #column2: something
         #column3:
     #delete: [t3, t4, t5]
  def initialize(config_name, adapter, logger, opts)
    config = Configuration.load(config_name)
    adapter = adapter.load(config, logger, opts)
  end

  def mask
    adapter.mask
    #adapter.ensure_tables
    #adapter.ensure_columns
    #adapter.mask
    #adapter.truncate
  end
end
