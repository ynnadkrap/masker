require 'spec_helper'

describe ::Masker::Adapters::Postgres do
  context 'with mocked database' do
    before do
      expect(PG::Connection).to receive(:new).with(nil).and_return(pg_mock)
    end

    let!(:pg_mock) { instance_double(PG::Connection) }
    let!(:subject) { described_class.new(Configuration.load('spec/configurations/test.yml'), nil) }

    describe '#remove_temp_tables' do
      it 'deletes all temp tables' do
        expect(pg_mock).to receive(:exec).with('DROP TABLE IF EXISTS temp_users;')
        subject.remove_temp_tables
      end
    end
  end
end
