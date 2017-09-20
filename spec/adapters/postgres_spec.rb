require 'spec_helper'

describe ::Masker::Adapters::Postgres do
  context 'with mocked database' do
    let(:logger_mock) { double(:logger) }
    let(:pg_mock) { instance_double(PG::Connection) }
    let(:subject) { described_class.new(Configuration.load('spec/configurations/test.yml'), logger_mock) }

    before do
      expect(PG::Connection).to receive(:new).with(nil).and_return(pg_mock)
    end

    describe '#insert_fake_data_into_temp_tables' do
      before do
        allow_any_instance_of(::Masker::Adapters::Postgres).to receive(:ids_to_mask).and_return({ 'users' => ['1'] })
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
