require 'rubygems'
require 'eventmachine'

EM.run{
  
  module Proxy
    def initialize
      # p [:init]
    end
    def post_init
      # p [:connected]
    end
    def receive_data data
      p [:receive, data]
    end
    def send_data data
      p [:send, data]
      super
    end
    def unbind
      # p [:disconnected]
    end
  end
  
  EM.start_server '127.0.0.1', 27017, Proxy
  
}
