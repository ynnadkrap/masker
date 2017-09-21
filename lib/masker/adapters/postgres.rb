require 'pg'

module Masker
  module Adapters
    class Postgres
      def initialize(config, logger, opts = {})
        @conn = PG::Connection.new(config['database_url'])
        @parser = ConfigParsers::Sql.new(config, conn, opts)
        @logger = logger
      end

      def mask
        remove_temp_tables
        parser.remove_missing_tables
        parser.remove_missing_columns
        create_temp_tables
        insert_fake_data_into_temp_tables
        merge_tables
        truncate
      ensure
        remove_temp_tables
      end

      private

      def remove_temp_tables
        tables.keys.each do |table_name|
          conn.exec("DROP TABLE IF EXISTS temp_#{table_name};")
        end
      end

      def create_temp_tables
        tables.keys.each do |table_name|
          conn.exec("CREATE TABLE temp_#{table_name} AS SELECT * FROM #{table_name} LIMIT 0;")
        end
      end

      def insert_fake_data_into_temp_tables
        tables.each do |table, columns|
          conn.transaction do |conn|
            parser.ids_to_mask[table].each_slice(1000) do |ids|
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
        columns.map { |_, mask_type| %Q['#{DataGenerator.generate(mask_type)}'] }
          .unshift(id)
          .map { |x| x.nil? ? 'NULL' : x }
          .join(", ")
      end

      def merge_tables
        tables.each do |table, columns|
          set_statement = columns.keys.map { |column| "#{column} = temp_#{table}.#{column}" }.join(", ")
          conn.exec("UPDATE #{table} SET #{set_statement} FROM temp_#{table} WHERE #{table}.id = temp_#{table}.id;")
        end
      end

      def truncate
        parser.tables_to_truncate.each do |table|
          conn.exec("TRUNCATE #{table};")
        end
      end

      def tables
        parser.tables
      end

      attr_reader :logger, :conn, :parser
    end
  end
end
