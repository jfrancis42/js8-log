#!/usr/bin/env ruby
# coding: utf-8

require 'csv'
require 'json'
require 'redis'
require 'pry_debug'

redis=Redis.new()

#hams=CSV.read("EN.short.dat", {:col_sep => "|", :liberal_parsing => true })
hams=CSV.read("EN.dat", {:col_sep => "|", :liberal_parsing => true })
hams.each do |ham|
  if(ham[4])
    json={:call => ham[4],
          :full => ham[7],
          :first => ham[8],
          :middle => ham[9],
          :last => ham[10],
          :street => ham[15],
          :city => ham[16],
          :state => ham[17],
          :zip => ham[18]
         }.to_json()
    puts("#{ham[4]}: #{json}")
    redis.set("ham/"+ham[4],json)
  end
end
