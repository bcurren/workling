require File.dirname(__FILE__) + '/test_helper'
require 'workling/clients/starling_priority_queue_client'

context "The starlingpriorityqueue client" do
  setup do
    Workling.send :class_variable_set, "@@config", { :listens_on => "localhost:12345" }
    @client = Workling::Clients::StarlingPriorityQueueClient.new
    @client.connect
  end
  
  specify "should have connection to starling" do
    @client.connection.class.should == ::Starling
  end
  
  specify "gets the highest (lowest number) priority for a given key." do
    @client.request('SomeKey',{:value => '1/0', :priority => 1})
    @client.request('SomeKey',{:value => '0/0', :priority => 0})
    @client.request('SomeKey',{:value => '0/1', :priority => 0})
    @client.request('SomeKey',{:value => '1/1', :priority => 1})

    @client.retrieve('SomeKey').should == {:value => '0/0', :priority => 0}
    @client.retrieve('SomeKey').should == {:value => '0/1', :priority => 0}
    @client.retrieve('SomeKey').should == {:value => '1/0', :priority => 1}
    @client.retrieve('SomeKey').should == {:value => '1/1', :priority => 1}
  end
end
