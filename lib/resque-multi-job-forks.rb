require 'resque'
require 'resque/worker'

module Resque
  class Worker
    attr_accessor :seconds_per_fork
    attr_accessor :jobs_per_fork
    attr_reader   :jobs_processed

    def send_child_process_signal(signal)
      if @child
        log! "Killing child at #{@child}"
        if system("ps -o pid,state -p #{@child}")
          Process.kill(signal, @child) rescue nil
        end
      end
    end

    unless method_defined?(:shutdown_without_multi_job_forks)
      def perform_with_multi_job_forks(job = nil)
        perform_without_multi_job_forks(job)
        hijack_fork unless fork_hijacked?
        @jobs_processed += 1
      end
      alias_method :perform_without_multi_job_forks, :perform
      alias_method :perform, :perform_with_multi_job_forks

      def shutdown_with_multi_job_forks?
        # if this gets changed due to a signal handler we need to
        # release the fork
        #
        # if the fork has been hijacked and we have reached our limit
        # then we need to again release the fork
        #
        # if the fork is hijacked? when we are in the child process
        release_fork if fork_hijacked? && (shutdown_without_multi_job_forks? || fork_job_limit_reached?)
        shutdown_without_multi_job_forks?
      end
      alias_method :shutdown_without_multi_job_forks?, :shutdown?
      alias_method :shutdown?, :shutdown_with_multi_job_forks?

      def shutdown_with_multi_job_forks
        send_child_process_signal("QUIT")
        shutdown_without_multi_job_forks
      end
      alias_method :shutdown_without_multi_job_forks, :shutdown
      alias_method :shutdown, :shutdown_with_multi_job_forks

      def pause_processing_with_multi_job_forks
        send_child_process_signal("USR2")
        pause_processing_without_multi_job_forks
      end
      alias_method :pause_processing_without_multi_job_forks, :pause_processing
      alias_method :pause_processing, :pause_processing_with_multi_job_forks

      def unpause_processing_with_multi_job_forks
        send_child_process_signal("CONT")
        unpause_processing_without_multi_job_forks
      end
      alias_method :unpause_processing_without_multi_job_forks, :unpause_processing
      alias_method :unpause_processing, :unpause_processing_with_multi_job_forks
    end

    def fork_hijacked?
      @release_fork_limit
    end

    def hijack_fork
      log 'hijack fork.'
      @suppressed_fork_hooks = [Resque.after_fork, Resque.before_fork]
      Resque.after_fork = Resque.before_fork = nil
      @release_fork_limit = fork_job_limit
      @jobs_processed = 0
      @cant_fork = true
    end

    def release_fork
      log "jobs processed by child: #{jobs_processed}"
      run_hook :before_child_exit, self
      Resque.after_fork, Resque.before_fork = *@suppressed_fork_hooks
      @release_fork_limit = @jobs_processed = @cant_fork = nil
      log 'hijack over, counter terrorists win.'
      @shutdown = true unless $TESTING
      # only the child calls release_fork, and it should exit and not
      # passively shutdown otherwise it will call unregister worker
      # after the work loop
      exit!
    end

    def fork_job_limit
      jobs_per_fork.nil? ? Time.now.to_i + seconds_per_fork : jobs_per_fork
    end

    def fork_job_limit_reached?
      fork_job_limit_remaining <= 0 ? true : false
    end

    def fork_job_limit_remaining
      jobs_per_fork.nil? ? @release_fork_limit - Time.now.to_i : jobs_per_fork - @jobs_processed
    end

    def seconds_per_fork
      @seconds_per_fork ||= minutes_per_fork * 60
    end

    def minutes_per_fork
      ENV['MINUTES_PER_FORK'].nil? ? 1 : ENV['MINUTES_PER_FORK'].to_i
    end

    def jobs_per_fork
      @jobs_per_fork ||= ENV['JOBS_PER_FORK'].nil? ? nil : ENV['JOBS_PER_FORK'].to_i
    end
  end

  # the `before_child_exit` hook will run in the child process
  # right before the child process terminates
  #
  # Call with a block to set the hook.
  # Call with no arguments to return the hook.
  def self.before_child_exit(&block)
    block ? (@before_child_exit = block) : @before_child_exit
  end

  # Set the before_child_exit proc.
  def self.before_child_exit=(before_child_exit)
    @before_child_exit = before_child_exit
  end

end
