# The script that fills the feed data into the HBase table
# Looks for xml files in the feeds directory

require 'rubygems'
require 'simple-rss'
require 'open-uri'
require 'digest/md5'

require_relative './lib/hbase_table.rb'
require_relative './lib/hbase_put.rb'

# Methods, needs to be an Object in the near future.
def generate_row_key link
  # rowkey is 'md5_hashed_link'
  d = Digest::MD5.new
  d.hexdigest link
end

add_counter = 0
table = HBaseTable.new("links")

Dir.glob(File.join(File.dirname(__FILE__),"feeds","*.xml")).each do |feed_path|
    
  puts "***** START OF #{feed_path} *****"
      
  #next unless feed_path =~ /reddit/i
  open(feed_path,"r") do |istream|
    rss = SimpleRSS.parse(istream)
    feed_title = rss.title
    rss.items.each do |itm|
      link = itm.link.strip
      
      next if link =~ /reddit\.com/i #ignore reddit self-posts
      #don't ignore too much for now
      #next if link =~ /imgur\.com/i #ignore image links
      #next if link =~ /youtube\.com/i #ignore youtube links
      #next if link =~ /github\.com/i #ignore github links
      #next if link =~ /\.(gif|jpg|jpeg|png)$/i #ignore image links
                                    
      title = itm.title.strip

      if feed_title =~ /reddit/i
        title_a = title.sub(/\)$/,'').rpartition(/\ \(/)
        title_string = title_a[0]
        points, comments = title_a[2].split(/;/)
        points_num = points.scan(/\d+/)[0].to_s unless points.nil?
        comments_count = comments.scan(/\d+/)[0].to_s unless comments.nil?
     else
        title_string = title
      end

      date = nil
      date ||= itm.pubDate.strftime('%d.%m.%Y %H:%M:%S') unless itm.pubDate.nil?
      author = "not available"
      author ||= itm.author 
      description = itm.description.gsub(/\&lt;/, "<").gsub(/\&gt;/, ">")

      rowkey =  generate_row_key link
      
      p = HBasePut.new(rowkey)
      
      ###add the core data
      p.add("core", "link", link)
      p.add("core", "title", title_string)
      ###add the meta
      p.add("meta", "feed_title", feed_title)
      p.add("meta", "comments", comments_count) unless comments_count.nil?
      p.add("meta", "points", points_num) unless points_num.nil?
      p.add("meta", "pdate", date) unless date.nil?
      p.add("meta", "author", author) 
      p.add("meta", "desc", description)

      #put it
      table.put(p)

      puts "Added ##{rowkey} published #{date}"
      add_counter += 1
    end
  end
                                                    
  puts "***** END OF #{feed_path} *****"
end
puts "#{add_counter} links added."
