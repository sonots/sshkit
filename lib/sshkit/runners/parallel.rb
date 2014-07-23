require 'parallel'

module SSHKit

  module Runner

    class Parallel < Abstract
      def execute
        ::Parallel.each(hosts, :in_processes => hosts.size) do |host|
          begin
            backend(host, &block).run
          rescue Exception => e
            e2 = ExecuteError.new e
            raise e2, "Exception while executing on host #{host}: #{e.message}" 
          end
        end
      end
    end

  end

end
