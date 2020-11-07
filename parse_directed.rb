#!/usr/bin/env ruby
# coding: utf-8

require 'time'
require 'json'
require 'csv'
require 'maidenhead'
require 'redis'
require 'pry_debug'

mycall="N0GQ"
okfreq_1=7120000
okfreq_2=10141500
#okfreq_1=7078000
#okfreq_1=7110000
bw_1=3000
bw_2=3000
info=Hash.new()
status=Hash.new()
heard=Hash.new()
grids=Hash.new()

$redis=Redis.new
$hams=Hash.new

def getham(call)
  stuff=$redis.get("ham/"+call.upcase)
  if(stuff)
    return(JSON.parse(stuff))
  else
    return(nil)
  end
end

# This data is (relatively) static, so we're going to cache the hell
# out of it for performance.
def hamloc(call)
  if($hams[call])
    ham=$hams[call]
  else
    ham=getham(call)
    $hams[call]=ham
  end
  if(ham)
    first=ham['first']
    if(first)
      return(first+" in "+ham['city']+", "+ham['state'])
    else
      return(ham['city']+", "+ham['state'])
    end
  else
    return("Unknown")
  end
end

# Read the directed text file.
File.readlines("DIRECTED.TXT").each do |line|
  # Split the line up into it's constituent parts (sure would be
  # cleaner to log in JSON).
  thing=line.split(" ",6)

  # Extract some useful stuff.
  date=thing[0]
  time=thing[1]
  freq=thing[2].to_f*1000000
  snr=thing[4]
  grid=nil
  lat=nil
  lon=nil

  # If there's actually some text to read. We do some really arcane
  # tests here to catch some really strange stuff. For example,
  # there's a line in my sample data where the SNR is simply missing
  # entirely. Bug in JS8Call, I assume. But it trashes the parsing.
  if(thing[5])
    if((!thing[5].include?("…"))&&(thing[5].include?("♢"))&&
       (freq.to_i>0)&&(date.include?('-'))&&(time.include?(':'))&&
       (((snr[0]=='+')||snr[0]=='-')&&(freq>=okfreq_1)&&(freq<=okfreq_1+bw_1)||
        ((snr[0]=='+')||snr[0]=='-')&&(freq>=okfreq_2)&&(freq<=okfreq_2+bw_2)))

      # Grab timestamp.
      local_time_t=Time.parse(date+" "+time+" GMT").to_i

      # Trim off the EOM marker, clean up the message content, split it
      # up into words, and store it in a reversed array so we can start
      # popping things off for analysis.
      stuff=thing[5].gsub('♢','').strip.split.reverse

      # Clear all the vars.
      from=nil
      to=nil
      from_relay=nil
      to_relay=nil

      # The from call will always be first.
      from=stuff.pop

      # Now it starts getting weird. JS8Call allows for spaces in the
      # call sign (dude, WTF?). Sometimes people separate out the "/P"
      # or something with a space, so keep pulling stuff until you find
      # the ':'. This stuff is all trash (for our purposes). Once we
      # find the trailing ':', we're done.  Clean off the "/whatever"
      # bits (if there are any), and we're left with our from call.
      # Well, maybe. Unless it's an intermediate relay. We'll deal with
      # that below.
      crap=from
      while(!crap.include?(':'))
        crap=stuff.pop
      end
      from=(from.split('/'))[0].gsub(':','')
      heard[from]=local_time_t

      # After the ':' word at the beginning, the next word is the to
      # call. It may or may not be the final to call; it could be an
      # intermediate relay. We'll figure that out.
      to=stuff.pop
      if(stuff[1]=="*DE*")
        from_relay=from
        heard[from_relay]=local_time_t
        from=(stuff[0].gsub('>','').split('/'))[0]
        heard[from]=local_time_t
        stuff=stuff[2..]
      end
      to=(to.gsub('>','').split('/'))[0]
      heard[to]=local_time_t

      # Now grab the next word in the message payload. If it ends in
      # '>', it's the actual to call, and what we stored as the to
      # previously is actually a relay. If it has a '>' in the middle
      # of the word, then in theory, it's the actual to call munged
      # together with the first word of the payload because the luser
      # didn't leave a space in his message after the to call. We have
      # no reasonable way to disambiguate that from the first word of
      # the message not being a call but having an embedded '>', so
      # we'll assume the former, split the pieces, and push the bit of
      # text after the '>' back into the message. If there's no '>' at
      # all, then we grabbed the first word of the actual message
      # payload, so we'll push it back. It's possible there's no more
      # text at this point (ie, it's an empty message).
      if(stuff.length>0)
        tmp=stuff.pop
        if(tmp[-1]=='>')
          to_relay=to
          heard[to_relay]=local_time_t
          to=(tmp.gsub('>','').split('/'))[0]
          heard[to]=local_time_t
        elsif(tmp.include?('>'))
          to_relay=to
          thing=tmp.split('>',2)
          to=thing[0].split('/')[0]
          heard[to]=local_time_t
          stuff.push(thing[1])
        else
          stuff.push(tmp)
        end
        
        # In theory, we now know to, from, to_relay (if any), and
        # from_relay (if any). The rest of the message payload is our
        # actual message. The first word of that might or might not be a
        # command (MSG, HEARTBEAT, GRID?, or any one of a list of others.
        if(stuff[-1]=='GRID')
          grid=stuff[-2]
        end
        if(stuff[-1]=='INFO')
          info[from]=stuff.reverse[1..].join(' ')
        end
        if(stuff[-1]=='STATUS')
          status[from]=stuff.reverse[1..].join(' ')
        end

        # ACK, AGN?, @ALLCALL, APRS::SMSGTE (???), CMD, CQ, GRID,
        # GRID?, HEARING, HEARING?, HEARTBEAT, INFO, INFO?, MSG, NACK
        # (???), SNR, SNR?, STATUS, STATUS?, QUERY MSGS, QUERY MSG
        # <num>, QUERY <call>?

        # odd:
        # Date: 2020-10-09
        # Time: 18:04:25
        # From: K7CDZ
        # To: N0GQ
        # Freq: 7078.0
        # SNR: +00
        # Text: @ALLCALL QUERY CALL K4NDZ?

        text=stuff.reverse.join(' ')
      else
        text=""
      end

      # Show us what we've got.
      puts("Time: #{Time.at(local_time_t)}")
      puts("From: #{from} (#{hamloc(from)})")
      if(from_relay)
        puts("From Relay: #{from_relay} (#{hamloc(from_relay)})")
      end
      puts("To: #{to} (#{hamloc(to)})")
      if(to_relay)
        puts("To Relay: #{to_relay} (#{hamloc(to_relay)})")
      end
      puts("Freq: #{freq}")
      puts("SNR: #{snr}")
      if(grid)
        # somebody has a "," in their grid...
        grids[from]=grid.gsub(',','')
        begin
          (lat,lon)=Maidenhead.to_latlon(grid)
        puts("Grid: #{grid}")
        rescue ArgumentError
          puts("Error: invalid grid")
        else
          puts("Lat: #{lat}")
          puts("Lon: #{lon}")
        end
      end
#      if(info)
#        puts("Info: #{info}")
#      end
#      if(status)
#        puts("Status: #{status}")
#      end
      puts("Text: #{text}")
      puts()
    end
  end
end

now=Time.now.to_i

puts("Stations with INFO:")
heard.sort_by{|call,time| time}.reverse.each do |n|
  unless(n[0][0]=="@")
    if(info[n[0]])
      puts("Call: #{n[0]}\tAge: #{now-heard[n[0]]}s\tGrid: #{if(grids[n[0]]); grids[n[0]]; else; "Unknown"; end}\tInfo: #{info[n[0]]}")
    end
  end
end

puts()
puts("Stations without INFO:")
heard.sort_by{|call,time| time}.reverse.each do |n|
  unless(n[0][0]=="@")
    unless(info[n[0]])
      puts("Call: #{n[0]}\tAge: #{now-heard[n[0]]}s\tGrid: #{grids[n[0]]}")
    end
  end
end

puts()
puts("Stations with without GRID:")
heard.sort_by{|call,time| time}.reverse.each do |n|
  unless(n[0][0]=="@")
    unless(grids[n[0]])
      puts("Call: #{n[0]}\tAge: #{now-heard[n[0]]}s")
    end
  end
end

File.open("hams.csv", 'w') do |file|
  file.puts("Call,Grid,Lat,Lon,Info,Status")
  heard.sort_by{|call,time| time}.reverse.each do |n|
    if((n[0][0]!="@")&&(grids[n[0]]))
      loc=Maidenhead.to_latlon(grids[n[0]])
      file.puts("#{n[0]},#{grids[n[0]]},#{loc[0]},#{loc[1]},#{info[n[0]]},#{status[n[0]]}")
    end
  end
end




  

#binding.pry
exit






# 2020-10-06 09:46:43     7.078000        1500    +02     KE4BML: W7SUA> KM4ACK>SOUND CARD SETTINGS WORKING TNX ♢ 
# 2020-10-07 10:50:59     7.078000        1344    -12     KC1GTU: N6GRG> 4F1BYN>QSL? ♢ 

# 2020-10-05 22:23:27     7.078000        1500    +03     K3VIN: AF5AV> K8GIB>TESTING ♢ 
# 2020-10-05 22:24:58     7.078000        1500    +10     K8GIB: AF5AV> K3VIN ACK ♢ 

# 2020-10-11 19:55:01     7.078000        1996    -11     K8GIB: W4OSS> K3VIN ACK ♢ 
# 2020-10-11 19:55:43     7.078000        1996    -19     W4OSS: K3VIN> ACK *DE* K8GIB ♢ 

# 2020-10-04 12:18:00     7.078000        1801    -24     JP8I M/P: MX7TST> 5 ♢ 

# 2020-10-07 10:50:59     7.078000        1344    -12     KC1GTU: N6GRG> 4F1BYN>QSL? ♢ 
# 2020-10-07 10:51:42     7.078000        1344    -08     N6GRG: 4F1BYN> QSL? *DE* KC1GTU ♢ 

# 2020-10-12 16:15:16     7.078000        1996    -17     AK7SJN/P: S88HYF/P> -19 ♢ 

# 2020-10-14 13:36:59     7.078000        1996    +11     N1CL: K7CDZ> K7CDZ MSG GDAY FROM MT EXPAT IN VK *DE* VK2XOR ♢ 

# 2020-10-17 01:34:59     7.078000        850     +04     KC1GTU: CD3IRD> LU1DIE>INFO? ♢ 
