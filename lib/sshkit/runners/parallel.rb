require 'parallel'

module SSHKit

  module Runner

    class Parallel < Abstract
      def execute
        opts = options[:concurrency] == 'processes' ? {in_processes: hosts.size} : {in_threads: hosts.size}
        ::Parallel.each(hosts, opts) do |host|
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
