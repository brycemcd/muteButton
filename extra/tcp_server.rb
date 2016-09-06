# shamelessly stolen from https://www.sitepoint.com/ruby-tcp-chat/
#!/usr/bin/env ruby -w
require "socket"
require 'securerandom'
class Server
  def initialize( port, ip )
    @server = TCPServer.open( ip, port )
    @connections = Hash.new
    @clients = Hash.new
    @connections[:server] = @server
    @connections[:clients] = @clients
    run
  end

  def run
    loop {
      Thread.start(@server.accept) do | client |
        nick_name = SecureRandom.hex(10)
        @connections[:clients].each do |other_name, other_client|
          if nick_name == other_name || client == other_client
            client.puts "This username already exist"
            Thread.kill self
          end
        end
        puts "#{nick_name} #{client}"
        @connections[:clients][nick_name] = client
        #client.puts "Connection established, Thank you for joining! Happy chatting"
        listen_user_messages( nick_name, client )
      end
    }.join
  end

  def listen_user_messages( username, client )
    loop {
      msg = client.gets.chomp
      @connections[:clients].each do |other_name, other_client|
        unless other_name == username
          #other_client.puts "#{username.to_s}: #{msg}"
          other_client.puts msg
        end
      end
    }
  end
end

Server.new( 9999, "10.1.2.230" )
