# Flume configuration

# Define the names of our sources, sinks, and channels
twitter.sources = TStream
twitter.sinks = HDFSSink
twitter.channels = MemoryChannel

# Configuration for our custom Flume Source -- put your access information here!
twitter.sources.TStream.type = xulu.TwitterSource
twitter.sources.TStream.channels = MemoryChannel
twitter.sources.TStream.consumerKey=<CONSUMER KEY>
twitter.sources.TStream.consumerSecret=<CONSUMER SECRET>
twitter.sources.TStream.accessToken=<ACCESS TOKEN>
twitter.sources.TStream.accessTokenSecret=<ACCESS TOKEN SECRET>
twitter.sources.TStream.keywords = hadoop, big data, analytics, bigdata, cloudera, data science, data scientist, business intelligence, mapreduce, data warehouse, data warehousing, mahout, hbase, nosql, newsql, businessintelligence, cloudcomputing

# Configure the HDFS sink. This will write files in 64 MB chunks, 
# or a new file every five minutes

# More information on the HDFS sink can be found at
# >http://flume.apache.org/FlumeUserGuide.html#hdfs-sink

twitter.sinks.HDFSSink.type = hdfs
twitter.sinks.HDFSSink.channel = MemoryChannel

# This will likely need to point to your NameNode, not mine!

#twitter.sinks.HDFSSink.hdfs.path = hdfs://Hadoop IP:9000/flume/twitter/location
twitter.sinks.HDFSSink.hdfs.path = hdfs://Hadoop IP:9000/flume/twitter/%Y/%m/%d/%H
twitter.sinks.HDFSSink.hdfs.filePrefix = twitter
twitter.sinks.HDFSSink.hdfs.fileSuffix = .json
twitter.sinks.HDFSSink.hdfs.rollInterval = 300
twitter.sinks.HDFSSink.hdfs.rollSize = 67108864
twitter.sinks.HDFSSink.hdfs.rollCount = 0
twitter.sinks.HDFSSink.hdfs.fileType = DataStream
twitter.sinks.HDFSSink.hdfs.writeFormat = Text

# A memory channel will flow data from Twitter to HDFS using memory,

# versus using a more fault-tolerant channel like a FileChannel

twitter.channels.MemoryChannel.type = memory
twitter.channels.MemoryChannel.capacity = 1000
twitter.channels.MemoryChannel.transactionCapacity = 1000