# PyMongoDump
A python script for replacing mongodump tool because of its slow speed.
Mongodump is very slow in copying data from one mongodb to another. Because it apply just one thread to one collection. 
If the collection is very large, the process will be very slow.
So We develope a python script to replace it with applying several processes to one collection if it is very big. 
After Testing its speed is about 5 times faster than mongodump.
