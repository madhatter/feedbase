include Java

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.HColumnDescriptor
import org.apache.hadoop.hbase.HConstants
import org.apache.hadoop.hbase.HTableDescriptor
import org.apache.hadoop.hbase.client.HBaseAdmin
import org.apache.hadoop.hbase.client.HTable
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.client.Scan
import org.apache.hadoop.io.Text

raise "Must include search string as first arg" unless ARGV.length >= 1
search = ARGV[0]
puts search
conf = HBaseConfiguration.new

table = HTable.new(conf, "links")

scan = Scan.new
scanner = table.get_scanner(scan)
begin
  while row = scanner.next do
    row_key = Bytes.toString(row.getRow)
    link = Bytes.toString(row.getValue(Bytes.toBytes("core"),Bytes.toBytes("link")))
        
    next unless link =~ /#{search}/i

    p [row_key,link]
  end
ensure
    scanner.close
end
