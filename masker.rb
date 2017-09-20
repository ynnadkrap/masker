require 'masker/configuration'
require 'masker/adapters/postgres'

class Masker
  def initialize(config_name, adapter, logger, opts)
    config = Configuration.load(config_name)
    @adapter = adapter.load(config, logger, opts)
  end

  def mask
    adapter.mask
  end

  private

  attr_reader :adapter
end
