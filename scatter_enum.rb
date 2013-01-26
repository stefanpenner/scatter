require 'timeout'
require 'celluloid'

class Scatter
  include Enumerable

  class Worker
    include Celluloid

    def work(callable, queue)
      begin
        message = callable.call
      rescue
        message = $!
      ensure
        queue.enq message
      end
    end
  end

  def initialize(*callables)
    @threads = []
    @callables = callables
    @queue = Queue.new
    @timeout = 60
    @pool = Celluloid::Actor[:scatter_pool] ||= Worker.pool
  end

  def each(&block)
    prepare
    enumerate(&block)
  end

  def first_successful
    detect { |entry| !entry.is_a?(Exception) }
  end

private
  attr_reader :callables, :timeout, :queue, :pool

  def enumerate
    Timeout::timeout(timeout) do
      callables.length.times do
        yield queue.deq
      end
    end
  end

  def prepare
    callables.map do |callable|
      pool.work!(callable, queue)
    end
  end
end

def scatter(*actions)
  Scatter.new(*actions)
end
