require 'spec_helper'

describe ::Masker::ConfigParsers::Sql do
  let(:pg_mock) { instance_double(PG::Connection) }
  let(:config) { Configuration.load('spec/configurations/test.yml') }
  let(:subject) { described_class.new(config, pg_mock) }

  describe '#missing_tables' do
    before do
      expect(pg_mock).to receive(:exec).with(/tablename = users/).and_yield([{'exists' => 'f'}])
    end

    it 'returns an array of missing tables' do
      expect(subject.missing_tables).to match_array(['users'])
    end
  end

  describe '#missing_columns' do
    before do
      expect(pg_mock).to receive(:exec).with(/column_name=email/).and_yield([{'exists' => 't'}])
      expect(pg_mock).to receive(:exec).with(/column_name=name/).and_yield([{'exists' => 't'}])
      expect(pg_mock).to receive(:exec).with(/column_name=ssn/).and_yield([{'exists' => 'f'}])
    end

    it 'returns a hash of tables and missing columns' do
      expect(subject.missing_columns).to eq({ 'users' => ['ssn'] })
    end
  end

  describe '#ids_to_mask' do
    before do
      expect(pg_mock).to receive(:exec).with(/SELECT id FROM users/).and_yield(double(:result, values: [['1'], ['2']]))
    end

    it 'returns a hash of tables and ids to mask' do
      expect(subject.ids_to_mask).to eq({ 'users' => ['1', '2'] })
    end
  end
end
