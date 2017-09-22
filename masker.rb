class Masker
  def initialize(database_url:, config_path:, adapter: Adapters::Postgres.new, logger: NullObject.new, opts: {})
    @adapter = adapter.load(database_url, config_path, logger, opts)
  end

  def mask
    adapter.mask
  end

  private

  attr_reader :adapter
end
