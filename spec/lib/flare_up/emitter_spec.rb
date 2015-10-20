describe FlareUp::Emitter do

  describe '.error' do
    it 'should emit' do
      expect(FlareUp::Emitter).to receive(:err).with("\x1b[31mTEST_MESSAGE")
      FlareUp::Emitter.error('TEST_MESSAGE')
    end
  end

  describe '.success' do
    it 'should emit' do
      expect(FlareUp::Emitter).to receive(:out).with("\x1b[32mTEST_MESSAGE")
      FlareUp::Emitter.success('TEST_MESSAGE')
    end
  end

  describe '.warn' do
    it 'should emit' do
      expect(FlareUp::Emitter).to receive(:err).with("\x1b[33mTEST_MESSAGE")
      FlareUp::Emitter.warn('TEST_MESSAGE')
    end
  end

  describe '.info' do
    it 'should emit' do
      expect(FlareUp::Emitter).to receive(:out).with('TEST_MESSAGE')
      FlareUp::Emitter.info('TEST_MESSAGE')
    end
  end

  describe '.sanitize' do

    context 'when colorize output is disabled' do
      before do
        FlareUp::OptionStore.store_option(:colorize_output, false)
      end
      it 'should remove color codes' do
        expect(FlareUp::Emitter.send(:sanitize, "\x1b[31mHello, World")).to eq('Hello, World')
      end
    end

    context 'when a risky option is being output' do
      before do
        FlareUp::OptionStore.store_options({
            :aws_access_key => 'access',
            :aws_secret_key => 'secret',
            :redshift_username => 'user',
            :redshift_password => 'pass',
            :redshift_port => '1234'
          })
      end
      it 'should hide it' do
        expect(FlareUp::Emitter.send(:sanitize, 'accesssecretuserpass')).to eq('REDACTEDREDACTEDREDACTEDREDACTED')
      end
    end

  end

end
