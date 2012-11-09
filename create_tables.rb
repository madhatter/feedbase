include Java

Dir.glob(File.join(File.dirname(__FILE__),"javalib","*.jar")).each do |jar_path|
    require jar_path
end

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.HColumnDescriptor
import org.apache.hadoop.hbase.HConstants
import org.apache.hadoop.hbase.HTableDescriptor
import org.apache.hadoop.hbase.client.HBaseAdmin
import org.apache.hadoop.hbase.client.HTable
import org.apache.hadoop.io.Text

conf = HBaseConfiguration.new
tablename = "links"
desc = HTableDescriptor.new(tablename)
desc.addFamily(HColumnDescriptor.new("core"))
desc.addFamily(HColumnDescriptor.new("meta"))
desc.addFamily(HColumnDescriptor.new("stats"))
admin = HBaseAdmin.new(conf)
if admin.isTableAvailable(tablename)
    admin.disableTable(tablename)
      admin.deleteTable(tablename)
end
admin.createTable(desc)
tables = admin.listTables

p [:tables, tables]

