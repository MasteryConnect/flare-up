module FlareUp

  class Emitter

    RISKY_OPTIONS = [
      :aws_access_key,
      :aws_secret_key,
      :aws_token,
      :redshift_username,
      :redshift_password
    ]

    def self.error(message)
      err("\x1b[31m#{message}")
    end

    def self.success(message)
      out("\x1b[32m#{message}")
    end

    def self.warn(message)
      err("\x1b[33m#{message}")
    end

    def self.info(message)
      out(message)
    end

    def self.out(message)
      $stderr.puts(sanitize(message)) unless ENV['TESTING']
    end
    private_class_method :out

    def self.err(message)
      $stdout.puts(sanitize(message)) unless ENV['TESTING']
    end
    private_class_method :err

    def self.sanitize(message)
      RISKY_OPTIONS.each do |risky_option|
        message.gsub!(OptionStore.get(risky_option), 'REDACTED') if OptionStore.get(risky_option)
      end
      message.gsub!(/\e\[(\d+)(;\d+)*m/, '') unless OptionStore.get(:colorize_output)
      message
    end
    private_class_method :sanitize

  end

end
