require 'workling/clients/base'
Workling.try_load_an_amqp_client
Workling.try_load_bunny_amqp_client

#
#  An Ampq client
#
module Workling
  module Clients
    class BunnyAmqpClient < Workling::Clients::Base
      
      # starts the client. 
      def connect
        begin
          @bunny = Bunny.new
          @bunny.start
        rescue
          raise WorklingError.new("Couldn't start bunny amqp client, ensure the AMQP server is running.")
        end
      end
      
      # no need for explicit closing. when the event loop
      # terminates, the connection is closed anyway. 
      def close
        @bunny.stop
        # normal amqp does not require stopping
      end
      
      # subscribe to a queue
      def subscribe(key)
        begin
          @amq ||= MQ.new
        rescue
          raise WorklingError.new("Couldn't start amqp client, if you are running this a server, ensure the server is evented (can't think why you'd want to though!).")
        end
        @amq.queue(key).subscribe do |data|
          value = Marshal.load(data)
          yield value
        end
      end
      
      # request and retrieve work
      def retrieve(key)
        @bunny.queue(key).pop[:payload]
      end
      def request(key, value)
        data = Marshal.dump(value)
        @bunny.queue(key).publish(data)
      end
    end
  end
end
