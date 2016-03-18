describe FlareUp::Command::Copy do

  subject(:copy_without_token) do
    FlareUp::Command::Copy.new('TEST_TABLE_NAME', 'TEST_DATA_SOURCE', 'TEST_ACCESS_KEY', 'TEST_SECRET_KEY')
  end

  subject(:copy_with_token) do
    FlareUp::Command::Copy.new('TEST_TABLE_NAME', 'TEST_DATA_SOURCE', 'TEST_ACCESS_KEY', 'TEST_SECRET_KEY', 'TEST_TOKEN')
  end

  its(:table_name) { should == 'TEST_TABLE_NAME' }
  its(:data_source) { should == 'TEST_DATA_SOURCE' }
  its(:aws_access_key_id) { should == 'TEST_ACCESS_KEY' }
  its(:aws_secret_access_key) { should == 'TEST_SECRET_KEY' }
  its(:columns) { should == [] }
  its(:options) { should == '' }

  describe '#get_command' do
    context 'when no optional fields are provided' do
      it 'should return a basic COPY command' do
        expect(copy_without_token.get_command).to eq("COPY TEST_TABLE_NAME  FROM 'TEST_DATA_SOURCE' CREDENTIALS 'aws_access_key_id=TEST_ACCESS_KEY;aws_secret_access_key=TEST_SECRET_KEY' ")
      end
    end

    context 'with aws_token when no optional fields are provided' do
      it 'should return a basic COPY command with token added to credentials' do
        expect(copy_with_token.get_command).to eq("COPY TEST_TABLE_NAME  FROM 'TEST_DATA_SOURCE' CREDENTIALS 'aws_access_key_id=TEST_ACCESS_KEY;aws_secret_access_key=TEST_SECRET_KEY;token=TEST_TOKEN' ")
      end
    end

    context 'when column names are provided' do
      before do
        copy_without_token.columns = %w(column_name1 column_name2)
      end
      it 'should include the column names in the command' do
        expect(copy_without_token.get_command).to start_with('COPY TEST_TABLE_NAME (column_name1, column_name2) FROM')
      end
    end

    context 'when options are provided' do
      before do
        copy_without_token.options = 'OPTION1 OPTION2'
      end
      it 'should include the options in the command' do
        expect(copy_without_token.get_command).to end_with(' OPTION1 OPTION2')
      end
    end
  end

  describe '#columns=' do
    context 'when an array' do
      it 'should assign the property' do
        copy_without_token.columns = %w(column_name1 column_name2)
        expect(copy_without_token.columns).to eq(%w(column_name1 column_name2))
      end
    end

    context 'when not an array' do
      it 'should not assign the property and be an error' do
        copy_without_token.columns = %w(column_name1)
        expect {
          copy_without_token.columns = '_'
        }.to raise_error(ArgumentError)
        expect(copy_without_token.columns).to eq(%w(column_name1))
      end
    end
  end

  describe '#execute' do

    let(:conn) { instance_double('FlareUp::Connection') }

    context 'when successful' do
      before do
        expect(conn).to receive(:execute)
      end
      it 'should do something' do
        expect(copy_without_token.execute(conn)).to eq([])
      end
    end

    context 'when unsuccessful' do

      before do
        expect(conn).to receive(:execute).and_raise(exception, message)
      end

      context 'when there was an internal error' do

        let(:exception) { PG::InternalError }

        context 'when there was an error loading' do
          let(:message) { "Check 'stl_load_errors' system table for details" }
          before do
            allow(FlareUp::STLLoadErrorFetcher).to receive(:fetch_errors).and_return('FOO')
          end
          it 'should respond with a list of errors' do
            expect(copy_without_token.execute(conn)).to eq('FOO')
          end
        end

        context 'when there was an error with the S3 prefix' do
          let(:message) { "The specified S3 prefix 'test_filename.csv' does not exist" }
          it 'should be an error' do
            expect { copy_without_token.execute(conn) }.to raise_error(FlareUp::DataSourceError)
          end
        end

        context 'when the bucket is not in the same zone as the Redshift instance' do
          let(:message) { 'The bucket you are attempting to access must be addressed using the specified endpoint' }
          it 'should be an error' do
            expect { copy_without_token.execute(conn) }.to raise_error(FlareUp::OtherZoneBucketError)
          end
        end

        context 'when there was another kind of internal error' do
          let(:message) { '_' }
          it 'should respond with a list of errors' do
            expect { copy_without_token.execute(conn) }.to raise_error(PG::InternalError, '_')
          end
        end

        context 'when there is a syntax error in the command' do
          let(:message) { 'ERROR:  syntax error at or near "lmlkmlk3" (PG::SyntaxError)' }
          it 'should be error' do
            expect { copy_without_token.execute(conn) }.to raise_error(FlareUp::SyntaxError, 'Syntax error in the COPY command: [at or near "lmlkmlk3"].')
          end
        end
      end

      context 'when there was another type of error' do
        let(:exception) { PG::ConnectionBad }
        let(:message) { '_' }
        it 'should do something' do
          expect { copy_without_token.execute(conn) }.to raise_error(PG::ConnectionBad, '_')
        end
      end
    end
  end
end
