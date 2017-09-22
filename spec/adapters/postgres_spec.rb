require 'spec_helper'
require 'pg_tester'
require 'postgres_fake'

describe ::Masker::Adapters::Postgres do
  context 'with mock database' do
    let(:psql) { PgTester.new(database: 'test_db', user: 'test') }

    before do
      psql.setup
    end

    after do
      psql.teardown
    end

    describe '#mask' do
      let(:logger) { double(:logger) }
      let(:safe_user_id) { 2 }
      let(:config) { Configuration.load('spec/postgres.yml') }
      let(:opts) do
        {
          safe_ids: {
            users: [safe_user_id]
          }
        }
      end
      let(:db_url) do
        {
          dbname: psql.database,
          port: psql.port,
          host: psql.host
        }
      end

      before do
        PostgresFake.new(psql).setup
        expect(logger).to receive(:warn).with(/Table: non_existing_table exists in configuration but not in database/)
        expect(logger).to receive(:warn).with(/Column: phones:non_existing_column exists in configuration but not in database/)
        described_class.new(db_url, 'spec/postgres.yml', logger, opts).mask
      end

      it '' do
        binding.pry
        truncates_expected_tables
        removes_temp_tables
        does_not_mask_safe_ids
        masks_sensitive_data
      end

      def truncates_expected_tables
        config['truncate'].each do |table|
          res = psql.exec("SELECT COUNT(*) FROM #{table};")
          expect(res.getvalue(0,0).to_i).to eq 0
        end
      end

      def removes_temp_tables
        config['mask'].each do |table|
          res = psql.exec("SELECT EXISTS(SELECT 1 FROM pg_tables WHERE tablename = 'temp_#{table}')")
          expect(res.getvalue(0,0)).to eq 'f'
        end
      end

      def does_not_mask_safe_ids
        safe_values = PostgresFake::VALUES[:users][1].values.map(&:to_s).map { |v| v.tr("'", "") }
        res = psql.exec("SELECT * FROM users WHERE id = #{safe_user_id}")
        expect(res.values[0]).to match_array(safe_values)
      end

      def masks_sensitive_data
        PostgresFake::VALUES.keys.each do |table|
          PostgresFake::VALUES[table].each do |row|
            next if Array(opts.dig(:safe_ids, table)).include?(row[:id])
            res = psql.exec("SELECT * FROM #{table} WHERE id = #{row[:id]}")
            row.each do |col, val|
              val = val.to_s.tr("'", "")
              columns_to_mask = config['mask'][table.to_s].keys

              if columns_to_mask.include?(col.to_s)
                expect(res[0][col.to_s]).to_not eq val
              else
                expect(res[0][col.to_s]).to eq val
              end
            end
          end
        end
      end
    end
  end
end
