require 'rubygems'
require 'simple-rss'
require 'open-uri'
require 'digest/md5'

include Java

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.HColumnDescriptor
import org.apache.hadoop.hbase.HConstants
import org.apache.hadoop.hbase.HTableDescriptor
import org.apache.hadoop.hbase.client.HBaseAdmin
import org.apache.hadoop.hbase.client.HTable
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.io.Text

# Methods, needs to be an Object in the near future.
def generate_row_key link
  # rowkey is 'md5_hashed_link'
  d = Digest::MD5.new
  d.hexdigest link
end

add_counter = 0
conf = HBaseConfiguration.new
table = HTable.new(conf,"links")

Dir.glob(File.join(File.dirname(__FILE__),"feeds","*.xml")).each do |feed_path|
    
  puts "***** START OF #{feed_path} *****"
      
  next unless feed_path =~ /reddit/i
  open(feed_path,"r") do |istream|
    rss = SimpleRSS.parse(istream)
    feed_title = rss.title
    rss.items.each do |itm|
      link = itm.link.strip
      
      next if link =~ /reddit\.com/i #ignore reddit self-posts
      next if link =~ /imgur\.com/i #ignore image links
      next if link =~ /youtube\.com/i #ignore youtube links
      next if link =~ /github\.com/i #ignore github links
      next if link =~ /\.(gif|jpg|jpeg|png)$/i #ignore image links
                                    
      title = itm.title.strip

      title_a = title.sub(/\)$/,'').rpartition(/\ \(/)
      title_string = title_a[0]
      points, comments = title_a[2].split(/;/)

      puts title_string
      points_num = points.scan(/\d+/)[0].to_s unless points.nil?
      comments_count = comments.scan(/\d+/)[0].to_s unless comments.nil?
      puts points_num
      puts comments_count

      date = itm.pubDate.strftime('%d.%m.%Y %H:%M:%S')
      rowkey =  generate_row_key link
      puts "Added ##{rowkey} published #{date}"
      add_counter += 1
      
      ##add the core data
      #add the link
      key = Bytes.toBytes("#{rowkey}")
      family = Bytes.toBytes("core")
      column = Bytes.toBytes("link")
      value = Bytes.toBytes(link)
      table.put(Put.new(key).add(family,column,value))
      #add the link title
      family = Bytes.toBytes("core")
      column = Bytes.toBytes("title")
      value = Bytes.toBytes(title_string)
      table.put(Put.new(key).add(family,column,value))
 
      ##add the meta
      #add the feed title
      family = Bytes.toBytes("meta")
      column = Bytes.toBytes("feed_title")
      value = Bytes.toBytes(feed_title)
      table.put(Put.new(key).add(family,column,value))
      #add the comment count
      family = Bytes.toBytes("meta")
      column = Bytes.toBytes("comments")
      value = Bytes.toBytes(comments_count)
      table.put(Put.new(key).add(family,column,value))
      #add the points
      family = Bytes.toBytes("meta")
      column = Bytes.toBytes("points")
      value = Bytes.toBytes(points_num)
      table.put(Put.new(key).add(family,column,value))

    end
  end
                                                        
  puts "***** END OF #{feed_path} *****"
end

