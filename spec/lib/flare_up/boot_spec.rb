describe FlareUp::Boot do

  describe '.boot' do
    let(:connection) { instance_double('FlareUp::Connection') }
    let(:copy_command) { instance_double('FlareUp::Command::Copy') }
    let(:klass) { FlareUp::Command::Copy }

    before do
      allow(copy_command).to receive(:get_command)
    end

    context 'when there is an error connecting' do

      before do
        expect(FlareUp::Boot).to receive(:create_command).and_return(copy_command)
        expect(copy_command).to receive(:execute).and_raise(copy_command_error)
      end

      context 'when there is a DataSourceError' do
        let(:copy_command_error) { FlareUp::DataSourceError }
        it 'should handle the error' do
          expect(FlareUp::CLI).to receive(:bailout).with(1)
          expect { FlareUp::Boot.boot klass }.not_to raise_error
        end
      end

      context 'when there is a OtherZoneBucketError' do
        let(:copy_command_error) { FlareUp::OtherZoneBucketError }
        it 'should handle the error' do
          expect(FlareUp::CLI).to receive(:bailout).with(1)
          expect { FlareUp::Boot.boot klass }.not_to raise_error
        end
      end

      context 'when there is a OtherZoneBucketError' do
        let(:copy_command_error) { FlareUp::SyntaxError }
        it 'should handle the error' do
          expect(FlareUp::CLI).to receive(:bailout).with(1)
          expect { FlareUp::Boot.boot klass }.not_to raise_error
        end
      end

    end

    context 'when there is an error copying' do

      before do
        expect(FlareUp::Boot).to receive(:create_connection).and_return(connection)
        expect(connection).to receive(:execute).and_raise(connection_error)
      end

      context 'when there is a HostUnknownOrInaccessibleError' do
        let(:connection_error) { FlareUp::HostUnknownOrInaccessibleError }
        it 'should handle the error' do
          expect(FlareUp::CLI).to receive(:bailout).with(1)
          expect { FlareUp::Boot.boot klass }.not_to raise_error
        end
      end

      context 'when there is a TimeoutError' do
        let(:connection_error) { FlareUp::TimeoutError }
        it 'should handle the error' do
          expect(FlareUp::CLI).to receive(:bailout).with(1)
          expect { FlareUp::Boot.boot klass }.not_to raise_error
        end
      end

      context 'when there is a NoDatabaseError' do
        let(:connection_error) { FlareUp::NoDatabaseError }
        it 'should handle the error' do
          expect(FlareUp::CLI).to receive(:bailout).with(1)
          expect { FlareUp::Boot.boot klass }.not_to raise_error
        end
      end

      context 'when there is a AuthenticationError' do
        let(:connection_error) { FlareUp::AuthenticationError }
        it 'should handle the error' do
          expect(FlareUp::CLI).to receive(:bailout).with(1)
          expect { FlareUp::Boot.boot klass }.not_to raise_error
        end
      end

      context 'when there is a UnknownError' do
        let(:connection_error) { FlareUp::UnknownError }
        it 'should handle the error' do
          expect(FlareUp::CLI).to receive(:bailout).with(1)
          expect { FlareUp::Boot.boot klass }.not_to raise_error
        end
      end

    end

  end

  describe '.create_connection' do
    before do
      FlareUp::OptionStore.store_options(
        {
          :redshift_endpoint => 'TEST_REDSHIFT_ENDPOINT',
          :database => 'TEST_DATABASE',
          :redshift_username => 'TEST_REDSHIFT_USERNAME',
          :redshift_password => 'TEST_REDSHIFT_PASSWORD',
          :redshift_port => 5439
        }
      )
    end

    it 'should create a connection' do
      connection = FlareUp::Boot.send(:create_connection)
      expect(connection.host).to eq('TEST_REDSHIFT_ENDPOINT')
      expect(connection.dbname).to eq('TEST_DATABASE')
      expect(connection.user).to eq('TEST_REDSHIFT_USERNAME')
      expect(connection.password).to eq('TEST_REDSHIFT_PASSWORD')
      expect(connection.port).to eq(5439)
    end
  end

  describe '.create_command' do

    before do
      FlareUp::OptionStore.store_options(
        {
          :table => 'TEST_TABLE',
          :data_source => 'TEST_DATA_SOURCE',
          :aws_access_key => 'TEST_ACCESS_KEY',
          :aws_secret_key => 'TEST_SECRET_KEY'
        }
      )
    end

    it 'should create a proper copy command' do
      command = FlareUp::Boot.send(:create_command, FlareUp::Command::Copy)
      expect(command.table_name).to eq('TEST_TABLE')
      expect(command.data_source).to eq('TEST_DATA_SOURCE')
      expect(command.aws_access_key_id).to eq('TEST_ACCESS_KEY')
      expect(command.aws_secret_access_key).to eq('TEST_SECRET_KEY')
    end

    context 'when aws token is provided' do
      before do
        FlareUp::OptionStore.store_option(:aws_token, 'TEST_TOKEN')
      end
      it 'should create a proper copy command' do
        command = FlareUp::Boot.send(:create_command, FlareUp::Command::Copy)
        expect(command.aws_token).to eq('TEST_TOKEN')
      end
    end

    context 'when columns are specified' do
      before do
        FlareUp::OptionStore.store_option(:column_list, ['c1'])
      end
      it 'should create a proper copy command' do
        command = FlareUp::Boot.send(:create_command, FlareUp::Command::Copy)
        expect(command.columns).to eq(['c1'])
      end
    end

    context 'when options are specified' do
      before do
        FlareUp::OptionStore.store_option(:copy_options, '_')
      end
      it 'should create a proper copy command' do
        command = FlareUp::Boot.send(:create_command, FlareUp::Command::Copy)
        expect(command.options).to eq('_')
      end
    end

  end

end
