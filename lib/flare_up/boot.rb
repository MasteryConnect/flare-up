module FlareUp

  class Boot

    # TODO: This control flow is untested and too procedural
    def self.boot
      conn = create_connection
      copy = create_copy_command

      begin
        trap('SIGINT') do
          Emitter.warn("\nCTRL-C received; cancelling COPY command...")
          error_message = conn.cancel_current_command
          if error_message
            Emitter.error("Error cancelling COPY: #{error_message}")
          else
            Emitter.success('COPY command cancelled.')
          end
          CLI.bailout(1)
        end

        Emitter.info("Executing command: #{copy.get_command}")
        handle_load_errors(copy.execute(conn))
      rescue ConnectionError => e
        Emitter.error(e.message)
        CLI.bailout(1)
      rescue CopyCommandError => e
        Emitter.error(e.message)
        CLI.bailout(1)
      end

    end

    def self.create_connection
      FlareUp::Connection.new(
        OptionStore.get(:redshift_endpoint),
        OptionStore.get(:database),
        OptionStore.get(:redshift_username),
        OptionStore.get(:redshift_password)
      )
    end
    private_class_method :create_connection

    def self.create_copy_command
      copy = FlareUp::CopyCommand.new(
        OptionStore.get(:table),
        OptionStore.get(:data_source),
        OptionStore.get(:aws_access_key),
        OptionStore.get(:aws_secret_key)
      )
      copy.columns = OptionStore.get(:column_list) if OptionStore.get(:column_list)
      copy.options = OptionStore.get(:copy_options) if OptionStore.get(:copy_options)
      copy
    end
    private_class_method :create_copy_command

    # TODO: Backfill tests
    def self.handle_load_errors(stl_load_errors)
      return if stl_load_errors.empty?
      Emitter.error("There was an error processing the COPY command.  Displaying the last (#{stl_load_errors.length}) errors.")
      stl_load_errors.each do |e|
        Emitter.error(e.pretty_print)
      end
      CLI.bailout(1)
    end
    private_class_method :handle_load_errors

  end

end