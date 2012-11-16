# HBasePut class is only a wrapper of Put Java class
# mainly because of the add method which should transfer all
# input into byte arrays by default
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

class HBasePut
  attr_reader :put 
  def initialize(rowkey)
    @put = Put.new(Bytes.toBytes(rowkey))
  end

  def add(family, column, value)
   family = Bytes.toBytes(family)
   column = Bytes.toBytes(column)
   value = Bytes.toBytes(value)
   @put.add(family,column,value)
  end
end 
