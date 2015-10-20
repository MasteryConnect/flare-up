module FlareUp

  class ConnectionError < StandardError
  end
  class HostUnknownOrInaccessibleError < ConnectionError
  end
  class TimeoutError < ConnectionError
  end
  class NoDatabaseError < ConnectionError
  end
  class AuthenticationError < ConnectionError
  end
  class UnknownError < ConnectionError
  end

  class Connection

    attr_accessor :host
    attr_accessor :port
    attr_accessor :dbname
    attr_accessor :user
    attr_accessor :password
    attr_accessor :connect_timeout

    def initialize(host, dbname, user, password, port)
      @host = host
      @dbname = dbname
      @user = user
      @password = password

      @port = port
      @connect_timeout = 5
    end

    def execute(statement)
      connection.async_exec(statement)
    end

    def cancel_current_command
      connection.cancel
    end

    private

    def connection
      @pg_conn ||= connect
    end

    def connect
      begin
        PG.connect(connection_parameters)
      rescue PG::ConnectionBad => e
        case e.message
          when /nodename nor servname provided, or not known/
            raise HostUnknownOrInaccessibleError, "Host unknown or unreachable: #{@host}"
          when /timeout expired/
            raise TimeoutError, "Timeout connecting to the database (have you checked your Redshift security groups?)"
          when /database ".+" does not exist/
            raise NoDatabaseError, "Database #{@dbname} does not exist"
          when /password authentication failed for user/
            raise AuthenticationError, "Either username '#{@user}' or password invalid"
          else
            raise UnknownError, e.message
        end
      end
    end

    # http://www.postgresql.org/docs/9.3/static/libpq-connect.html#LIBPQ-PARAMKEYWORDS
    def connection_parameters
      {
        :host => @host,
        :port => @port,
        :dbname => @dbname,
        :user => @user,
        :password => @password,
        :connect_timeout => @connect_timeout,
        # Enable keep-alives
        :keepalives => 1,
        # Idle time in between keep-alives when there is a response from the peer
        :keepalives_idle => 30,
        # Interval between keep-alives when there is no response from the peer
        # This is done to probe the peer until there is a response
        :keepalives_interval => 10,
        # Number of keep-alives that can be lost before the client's connection
        # to the server is considered dead
        :keepalives_count => 3
      }
    end

  end

end
