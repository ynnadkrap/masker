require 'pg'

module Masker
  module Adapters
    class Postgres
      def initialize(config, logger, opts = {})
        @config = config
        @logger = logger
        @opts = opts
        # TODO: this hangs with a string
        @conn = PG::Connection.new(config['database_url'])
      end

      def mask
        remove_temp_tables
        #ensure_tables
        #ensure_columns
        #create_temp_table
        #insert_fake_data_into_temp_table
        #update_tables
        #truncate
      #ensure
        #remove_temp_table
      end

      def remove_temp_tables
        tables_to_mask.keys.each do |table_name|
          conn.exec("DROP TABLE IF EXISTS temp_#{table_name};")
        end
      end

      private

      def tables_to_mask
        @tables_to_mask ||= config['mask']
      end

      attr_reader :config, :logger, :opts, :conn
    end
  end
end
