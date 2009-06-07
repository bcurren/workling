require 'starling'
require 'workling/clients/memcache_queue_client'

module Workling
  module Clients
    class StarlingPriorityQueueClient < Workling::Clients::MemcacheQueueClient
      
      def connect
        @queueserver_urls = Workling.config[:listens_on].split(',').map { |url| url ? url.strip : url }
        options = [@queueserver_urls, Workling.config[:memcache_options]].compact
        self.connection = Starling.new(*options)
        
        raise_unless_connected!
      end
      
      # implements the client job request and retrieval 
      def request(key, value)
        priority = value[:priority] || 0
        encoded_key = "#{key}_#{priority}"
        set(encoded_key, value)
      end
      
      def retrieve(key)
        begin
          queues = self.available_queues
          # If key exists, retrieve from queue
          return get(key) if queues.include?(key) && self.sizeof(key) > 0
          
          # Else, find the queue with the correct priority and retrieve from queue
          "0".upto("10") do |priority|   
            k = key+'_'+priority
            if queues.include?(k) && self.sizeof(k) > 0
              result = get(k)
              return result unless result.nil?
            end
          end
        rescue MemCache::MemCacheError => e
          # failed to enqueue, raise a workling error so that it propagates upwards
          raise Workling::WorklingError.new("#{e.class.to_s} - #{e.message}")        
        end
        
        nil
      end
    end
  end
end
