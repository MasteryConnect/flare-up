module FlareUp

  class CLI < Thor

    ### shared methods and variables

    no_commands {
      def command_setup(data_source, endpoint, database_name, table_name)
        boot_options = {
          :data_source => data_source,
          :redshift_endpoint => endpoint,
          :database => database_name,
          :table => table_name
        }
        options.each { |k, v| boot_options[k.to_sym] = v }

        begin
          CLI.env_validator(boot_options, :aws_access_key, 'AWS_ACCESS_KEY_ID')
          CLI.env_validator(boot_options, :aws_secret_key, 'AWS_SECRET_ACCESS_KEY')
          CLI.env_validator(boot_options, :redshift_username, 'REDSHIFT_USERNAME')
          CLI.env_validator(boot_options, :redshift_password, 'REDSHIFT_PASSWORD')
          CLI.env_validator(boot_options, :redshift_port, 'REDSHIFT_PORT', 5439)
        rescue ArgumentError => e
          Emitter.error(e.message)
          CLI.bailout(1)
        end

        OptionStore.store_options(boot_options)
      end

      def no_datasource_command(endpoint, database_name, table_name)
        command_setup(nil, endpoint, database_name, table_name)
        command = CLI.command_formatter __callee__
        Boot.boot command
      end
    }

    all_shared_options = [
      [:redshift_username, { :type => :string, :desc => "Required unless ENV['REDSHIFT_USERNAME'] is set." }],
      [:redshift_password, { :type => :string, :desc => "Required unless ENV['REDSHIFT_PASSWORD'] is set." }],
      [:redshift_port, { :type => :numeric, :desc => "Optional (defaults to 5439)" }],
      [:colorize_output, { :type => :boolean, :desc => 'Should Flare-up colorize its output?', :default => true }]
    ]
    copy_options = [
      [:aws_access_key, { :type => :string, :desc => "Required unless ENV['AWS_ACCESS_KEY_ID'] is set." }],
      [:aws_secret_key, { :type => :string, :desc => "Required unless ENV['AWS_SECRET_ACCESS_KEY'] is set." }],
      [:copy_options, { :type => :string, :desc => "Appended to the end of the command; enclose \"IN QUOTES\"" }]
    ]

    long_desc_footer = <<-LONGDESCFOOTER
\nDocumentation for this version can be found at:\x5
https://github.com/sharethrough/flare-up/blob/v#{FlareUp::VERSION}/README.md
    LONGDESCFOOTER

    ### copy command

    desc 'copy DATA_SOURCE REDSHIFT_ENDPOINT DATABASE TABLE', 'COPY data into REDSHIFT_ENDPOINT from DATA_SOURCE into DATABASE.TABLE'
    long_desc <<-LONGDESC
`flare-up copy` executes the Redshift COPY command, loading data from\x5
DATA_SOURCE into DATABASE_NAME.TABLE_NAME at REDSHIFT_ENDPOINT.
#{long_desc_footer}
    LONGDESC
    option :column_list, :type => :array, :desc => 'A space-separated list of columns, should your DATA_SOURCE require it'
    [*all_shared_options, *copy_options].each { |shared_options| method_option *shared_options }
    def copy(data_source, endpoint, database_name, table_name)
      command_setup(data_source, endpoint, database_name, table_name)
      command = CLI.command_formatter __method__
      Boot.boot command
    end

    ### create_table command

    desc 'create_table REDSHIFT_ENDPOINT DATABASE TABLE', 'CREATE DATABASE.TABLE in REDSHIFT_ENDPOINT'
    long_desc <<-LONGDESC
`flare-up create_table` executes the Redshift CREATE TABLE command, creating a table named\x5
TABLE in DATABASE_NAME at REDSHIFT_ENDPOINT, using the scmhema provided in --column-list.
#{long_desc_footer}
    LONGDESC
    option :column_list, { :type => :string, :desc => "Required. A space-separated list of columns with their data-types, enclose \"IN QUOTES\"" }
    [*all_shared_options].each { |shared_options| method_option *shared_options }
    alias_method :create_table, :no_datasource_command

    ### drop_table command

    desc 'drop_table REDSHIFT_ENDPOINT DATABASE TABLE', 'DROP DATABASE.TABLE in REDSHIFT_ENDPOINT'
    long_desc <<-LONGDESC
`flare-up drop_table` executes the Redshift DROP TABLE command, removing the table named TABLE\x5
in DATABASE_NAME at REDSHIFT_ENDPOINT.
#{long_desc_footer}
    LONGDESC
    all_shared_options.each { |shared_options| method_option *shared_options }
    alias_method :drop_table, :no_datasource_command

    ### truncate command

    desc 'truncate REDSHIFT_ENDPOINT DATABASE TABLE', 'TRUNCATE DATABASE.TABLE in REDSHIFT_ENDPOINT'
    long_desc <<-LONGDESC
`flare-up truncate` executes the Redshift TRUNCATE command, deleting all rows the table named TABLE\x5
in DATABASE_NAME at REDSHIFT_ENDPOINT.
#{long_desc_footer}
    LONGDESC
    all_shared_options.each { |shared_options| method_option *shared_options }
    alias_method :truncate, :no_datasource_command

    # transforms the symbol method names to the corresponding class under
    # the FlareUp::Command namespace
    def self.command_formatter(sym)
      name = sym.to_s.split('_').map(&:capitalize).join
      # Ruby 1.9.3 cannot handle const_get for nested Classes (Ruby 2.0 can).
      "FlareUp::Command::#{name}".split("::").reduce(Object) { |a, e| a.const_get e }
    end

    def self.env_validator(options, option_name, env_variable_name, default_value=nil)
      options[option_name] ||= (ENVWrap.get(env_variable_name) || default_value)
      return if options[option_name]
      raise ArgumentError, "One of either the --#{option_name} option or the ENV['#{env_variable_name}'] must be set"
    end

    def self.bailout(exit_code)
      exit(exit_code)
    end

  end

end
