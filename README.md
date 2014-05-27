MemoryMappedBitmaps
===================

Real data experiments that compare performances of Roaring bitmap 2.0 and ConciseSet 2.2 using memory mapped files.

Usage 
===================
* install java
* install maven 2
* execute : mvn package
* then : cd target
* then : java -cp MemoryMappedFiles-0.0.1-SNAPSHOT.jar:lib/* -javaagent:./lib/SizeOf.jar Benchmark ../real-roaring-datasets
