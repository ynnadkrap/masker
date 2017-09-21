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
      let(:safe_user_id) { 2 }
      let(:config) do
        config = Configuration.load('spec/configurations/postgres.yml')
        config['database_url'] = {
          dbname: psql.database,
          port: psql.port,
          host: psql.host
        }
        config
      end
      let(:opts) do
        {
          safe_ids: {
            users: [safe_user_id]
          }
        }
      end

      before do
        PostgresFake.new(psql).setup
        described_class.new(config, double(:logger), opts).mask
      end

      # TODO: test logging for remove_missing_tables/columns
      it '' do
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

  # This isn't ideal, but the method being tested is somewhat complex and I'd like to guarantee it works
  describe 'private_methods' do
    let(:logger_mock) { double(:logger) }
    let(:pg_mock) { instance_double(PG::Connection) }
    let(:subject) { described_class.new(Configuration.load('spec/configurations/test.yml'), logger_mock) }

    before do
      expect(PG::Connection).to receive(:new).with(nil).and_return(pg_mock)
    end

    describe '#insert_fake_data_into_temp_tables' do
      before do
        allow_any_instance_of(::Masker::ConfigParsers::Sql).to receive(:ids_to_mask).and_return({ 'users' => ['1'] })
        expect(::DataGenerator).to receive(:generate).with(:name).and_return('Rick Sanchez')
        expect(::DataGenerator).to receive(:generate).with(:email).and_return('r@s.rm.com')
        expect(::DataGenerator).to receive(:generate).with(nil).and_call_original
        expect(pg_mock).to receive(:transaction).and_yield(pg_mock)
        expect(pg_mock).to receive(:exec)
          .with("INSERT INTO temp_users (id, email, name, ssn) VALUES (1, r@s.rm.com, Rick Sanchez, NULL);")
      end

      it 'inserts the fake data row into temp table' do
        subject.send(:insert_fake_data_into_temp_tables)
      end
    end
  end
end
