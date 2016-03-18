describe FlareUp::CLI do

  let(:required_arguments) { %w(copy TEST_DATA_SOURCE TEST_REDSHIFT_ENDPOINT TEST_DATABASE TEST_TABLE) }
  let(:base_options) do
    {
      :data_source => 'TEST_DATA_SOURCE',
      :redshift_endpoint => 'TEST_REDSHIFT_ENDPOINT',
      :database => 'TEST_DATABASE',
      :table => 'TEST_TABLE',
      :redshift_username => 'TEST_REDSHIFT_USERNAME',
      :redshift_password => 'TEST_REDSHIFT_PASSWORD',
      :redshift_port => 5439,
      :aws_access_key => 'TEST_AWS_ACCESS_KEY',
      :aws_secret_key => 'TEST_AWS_SECRET_KEY',
      :aws_token => nil,
      :colorize_output => true
    }
  end

  before do
    allow(FlareUp::ENVWrap).to receive(:get).with('AWS_ACCESS_KEY_ID').and_return('TEST_AWS_ACCESS_KEY')
    allow(FlareUp::ENVWrap).to receive(:get).with('AWS_SECRET_ACCESS_KEY').and_return('TEST_AWS_SECRET_KEY')
    allow(FlareUp::ENVWrap).to receive(:get).with('AWS_TOKEN').and_return(nil)
    allow(FlareUp::ENVWrap).to receive(:get).with('REDSHIFT_USERNAME').and_return('TEST_REDSHIFT_USERNAME')
    allow(FlareUp::ENVWrap).to receive(:get).with('REDSHIFT_PASSWORD').and_return('TEST_REDSHIFT_PASSWORD')
    allow(FlareUp::ENVWrap).to receive(:get).with('REDSHIFT_PORT').and_return(nil)
    allow(FlareUp::Boot).to receive(:boot)
  end

  describe '#copy' do

    it 'should boot' do
      expect(FlareUp::Boot).to receive(:boot)
      FlareUp::CLI.start(required_arguments)
    end

    context 'when no options are specified' do
      it 'should boot with the base options' do
        FlareUp::CLI.start(required_arguments)
        expect(FlareUp::OptionStore.get_options).to eq(base_options)
      end
    end

    context 'when column ordering is specified' do
      it 'should boot with the proper options' do
        FlareUp::CLI.start(required_arguments + %w(--column_list c1 c2 c3))
        expect(FlareUp::OptionStore.get(:column_list)).to eq(%w(c1 c2 c3))
      end
    end

    context 'when COPY options are specified' do
      it 'should boot with the proper options' do
        FlareUp::CLI.start(required_arguments + ['--copy_options', 'TEST_COPY_OPTIONS WITH A SPACE'])
        expect(FlareUp::OptionStore.get(:copy_options)).to eq('TEST_COPY_OPTIONS WITH A SPACE')
      end
    end

    describe 'colorizing output' do

      context 'when it is not specified' do
        it 'should boot with the proper options' do
          FlareUp::CLI.start(required_arguments)
          expect(FlareUp::OptionStore.get(:colorize_output)).to eq(true)
        end
      end

      context 'when it is specified' do
        it 'should boot with the proper options' do
          FlareUp::CLI.start(required_arguments + ['--colorize_output=false'])
          expect(FlareUp::OptionStore.get(:colorize_output)).to eq(false)
        end
      end

    end

    describe 'Redshift credentials' do

      describe 'username' do
        context 'when it is specified on the CLI' do
          it 'should boot with the proper options' do
            FlareUp::CLI.start(required_arguments + %w(--redshift_username CLI_VALUE))
            expect(FlareUp::OptionStore.get(:redshift_username)).to eq('CLI_VALUE')
          end
        end
        context 'when it is not specified on the CLI' do
          context 'when it is available via ENV' do
            it 'should boot with the key from the environment' do
              FlareUp::CLI.start(required_arguments)
              expect(FlareUp::OptionStore.get(:redshift_username)).to eq('TEST_REDSHIFT_USERNAME')
            end
          end
          context 'when it is not available via ENV' do
            it 'should be an error' do
              allow(FlareUp::ENVWrap).to receive(:get).with('REDSHIFT_USERNAME').and_return(nil)
              expect(FlareUp::CLI).to receive(:bailout).with(1)
              FlareUp::CLI.start(required_arguments)
            end
          end
        end
      end

      describe 'password' do
        context 'when it is specified on the CLI' do
          it 'should boot with the proper options' do
            FlareUp::CLI.start(required_arguments + %w(--redshift_password CLI_VALUE))
            expect(FlareUp::OptionStore.get(:redshift_password)).to eq('CLI_VALUE')
          end
        end
        context 'when it is not specified on the CLI' do
          context 'when it is available via ENV' do
            it 'should boot with the key from the environment' do
              FlareUp::CLI.start(required_arguments)
              expect(FlareUp::OptionStore.get(:redshift_password)).to eq('TEST_REDSHIFT_PASSWORD')
            end
          end
          context 'when it is not available via ENV' do
            before do
            end
            it 'should be an error' do
              allow(FlareUp::ENVWrap).to receive(:get).with('REDSHIFT_PASSWORD').and_return(nil)
              expect(FlareUp::CLI).to receive(:bailout).with(1)
              FlareUp::CLI.start(required_arguments)
            end
          end
        end
      end

      describe 'port' do
        context 'when it is specified on the CLI' do
          it 'should boot with the proper options' do
            FlareUp::CLI.start(required_arguments + %w(--redshift_port 1234))
            expect(FlareUp::OptionStore.get(:redshift_port)).to eq(1234)
          end
        end
        context 'when it is not specified on the CLI' do
          context 'when it is available via ENV' do
            it 'should boot with the key from the environment' do
              allow(FlareUp::ENVWrap).to receive(:get).with('REDSHIFT_PORT').and_return(5678)
              FlareUp::CLI.start(required_arguments)
              expect(FlareUp::OptionStore.get(:redshift_port)).to eq(5678)
            end
          end
          context 'when it is not available via ENV' do
            it 'should boot with the default value' do
              allow(FlareUp::ENVWrap).to receive(:get).with('REDSHIFT_PORT').and_return(nil)
              FlareUp::CLI.start(required_arguments)
              expect(FlareUp::OptionStore.get(:redshift_port)).to eq(5439)
            end
          end
        end
      end

    end

    describe 'AWS credentials' do

      describe 'access key' do
        context 'when an AWS access key is specified' do
          it 'should boot with the proper options' do
            FlareUp::CLI.start(required_arguments + %w(--aws_access_key CLI_KEY))
            expect(FlareUp::OptionStore.get(:aws_access_key)).to eq('CLI_KEY')
          end
        end
        context 'when an AWS access key is not specified' do
          context 'when the key is available via ENV' do
            it 'should boot with the key from the environment' do
              FlareUp::CLI.start(required_arguments)
              expect(FlareUp::OptionStore.get(:aws_access_key)).to eq('TEST_AWS_ACCESS_KEY')
            end
          end
          context 'when the key is not available via ENV' do
            before do
              allow(FlareUp::ENVWrap).to receive(:get).with('AWS_ACCESS_KEY_ID').and_return(nil)
              expect(FlareUp::CLI).to receive(:bailout).with(1)
            end
            it 'should be an error' do
              FlareUp::CLI.start(required_arguments)
            end
          end
        end
      end

      describe 'secret key' do
        context 'when an AWS secret key is specified' do
          it 'should boot with the proper options' do
            FlareUp::CLI.start(required_arguments + %w(--aws_secret_key CLI_KEY))
            expect(FlareUp::OptionStore.get(:aws_secret_key)).to eq('CLI_KEY')
          end
        end
        context 'when an AWS secret key is not specified' do
          context 'when the key is available via ENV' do
            it 'should boot with the key from the environment' do
              FlareUp::CLI.start(required_arguments)
              expect(FlareUp::OptionStore.get(:aws_secret_key)).to eq('TEST_AWS_SECRET_KEY')
            end
          end
          context 'when the key is not available via ENV' do
            before do
              allow(FlareUp::ENVWrap).to receive(:get).with('AWS_SECRET_ACCESS_KEY').and_return(nil)
              expect(FlareUp::CLI).to receive(:bailout).with(1)
            end
            it 'should be an error' do
              FlareUp::CLI.start(required_arguments)
            end
          end
        end
      end

      describe 'token' do
        context 'when an AWS token is specified' do
          it 'should boot with the proper options' do
            FlareUp::CLI.start(required_arguments + %w(--aws_token CLI_KEY))
            expect(FlareUp::OptionStore.get(:aws_token)).to eq('CLI_KEY')
          end
        end
        before do
          allow(FlareUp::ENVWrap).to receive(:get).with('AWS_TOKEN').and_return('TEST_AWS_TOKEN')
        end
        context 'when an AWS token is not specified' do
          context 'when the token is available via ENV' do
            it 'should boot with the token from the environment' do
              FlareUp::CLI.start(required_arguments)
              expect(FlareUp::OptionStore.get(:aws_token)).to eq('TEST_AWS_TOKEN')
            end
          end
        end
      end

    end

  end

end
