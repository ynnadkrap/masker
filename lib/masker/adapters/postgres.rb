require 'pg'

module Masker
  module Adapters
    class Postgres
      def initialize(config, logger, opts = {})
        @config = config
        @logger = logger
        @opts = opts
        @tables_to_mask = config['mask']
        # TODO: figure out best way to pass in connection url
        @conn = PG::Connection.new(config['database_url'])
        @parser = Parsers::Postgres.new(config, conn, opts)
      end

      def mask
        remove_temp_tables
        ensure_tables
        ensure_columns
        create_temp_tables
        insert_fake_data_into_temp_table
        merge_tables
        truncate
      ensure
        remove_temp_table
      end

      private

      def remove_temp_tables
        tables_to_mask.keys.each do |table_name|
          conn.exec("DROP TABLE IF EXISTS temp_#{table_name};")
        end
      end

      def create_temp_tables
        tables_to_mask.keys.each do |table_name|
          connect.exec("CREATE TABLE temp_#{table_name} AS SELECT * FROM #{table_name} LIMIT 0;")
        end
      end

      def ensure_tables
        parser.missing_tables.each do |table|
          tables_to_mask.delete(table)
          logger.warn "#{table} exists in configuration but not in the database."
        end
      end

      def ensure_columns
        parser.missing_columns.each do |table, columns|
          columns.each do |column|
            tables_to_mask[table].delete(column)
            logger.warn "#{table}::#{column} exists in configuration but not in the database."
          end
        end
      end

      def insert_fake_data_into_temp_tables
        tables_to_mask.each do |table, columns|
          conn.transaction do |conn|
            ids_to_mask[table].each_slice(1000) do |ids|
              fake_rows = create_fake_rows(ids, columns)
              conn.exec("INSERT INTO temp_#{table} (id, #{columns.keys.join(", ")}) VALUES #{fake_rows};")
            end
          end
        end
      end

      def create_fake_rows(ids, columns)
        ids.map { |id| "(#{create_fake_row(id, columns)})" }.join(", ")
      end

      def create_fake_row(id, columns)
        columns.map { |_, mask_type| DataGenerator.generate(mask_type) }
          .unshift(id)
          .map { |x| x.nil? ? 'NULL' : x }
          .join(", ")
      end

      def merge_tables
        tables_to_mask.each do |table, columns|
          set_statement = columns.keys.map { |column| "#{column} = fake_#{table}.#{column}" }.join(", ")
          conn.exec("UPDATE #{table} SET #{set_statement} FROM fake_#{table} WHERE #{table}.id = fake_#{table_name}.id;")
        end
      end

      def truncate
        config['truncate'].each do |table|
          conn.exec("TRUNCATE #{table};")
        end
      end

      def ids_to_mask
        @ids_to_mask ||= parser.ids_to_mask
      end

      attr_reader :config, :logger, :opts, :conn, :tables_to_mask, :parser
    end
  end
end
