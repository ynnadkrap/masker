module Masker
  module Parsers
    class Postgres
      def initialize(config, conn, opts = {})
        @config = config
        @tables_to_mask = config['mask']
        @conn = conn
        @opts = opts
      end

      def ids_to_mask
        # TODO handle ids_to_not_mask in opts
        tables_to_mask.keys.each_with_object(Hash.new { |k, v| k[v] = [] }) do |table, ids|
          conn.exec("SELECT id FROM #{table};") do |result|
            ids[table].concat(result.values.map(&:first))
          end
        end
      end

      def missing_tables
        tables_to_mask.keys.each_with_object([]) do |table_name, missing_tables|
          conn.exec("SELECT EXISTS (SELECT 1 FROM pg_tables WHERE tablename = #{table_name});") do |result|
            missing_tables << table_name if result[0]['exists'] == 'f'
          end
        end
      end

      def missing_columns
        tables_to_mask.each_with_object(Hash.new { |h, k| h[k] = [] }) do |(table_name, columns), missing_columns|
          columns.keys.each do |column_name|
            sql = <<~SQL
              SELECT EXISTS (
                SELECT 1 FROM information_schema.columns
                WHERE table_name=#{table_name}
                AND column_name=#{column_name}
              );
            SQL
            conn.exec(sql) do |result|
              missing_columns[table_name] << column_name if result[0]['exists'] == 'f'
            end
          end
        end
      end

      private

      attr_reader :config, :tables_to_mask, :conn, :opts
    end
  end
end
