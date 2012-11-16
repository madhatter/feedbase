#feedbase

Playing around with jRuby and HBase.

Setup:
------
* Setting the classpath: export CLASSPATH=``hadoop classpath``:``hbase classpath``
* Use jRuby: `rvm jruby`

Usage:
------
* Create the table with `ruby create_tables.rb`
* Download some demo feeds with `ruby download_latest_feeds.rb`
* Put them in your HBase table with `ruby populate_links_from_feeds.rb`
* Print 'em out with `ruby show_data.rb`

Notes:
------
The _parser_ is optimized for the weekly reddit feeds at the moment. There may be feeds
that have more useful meta information that does not get parsed or that lacks lots of default
meta information like the Hacker News feed.

That's almost it. For now.

