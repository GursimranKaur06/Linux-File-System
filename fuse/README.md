# User Space File System In LINUX
This project creates a user space file system in LINUX using FUSE (File System in User Space). This is a replicated file system wherein each file is replicated at 2 different locations which supports fault tolerance and performance enhancement. The system starts with 2 mount points - ../master and ../slave_{i}. The master supports both read and write operations whereas the slave only supports read operations. Each change in the system is replicated in a synchronous manner. The system also allows the user to specify the number of replicas as wished. The read operations are distributed among the replicas in a round robin fashion.
There are essentially n+1 threads are created for n slaves, one for the master, and n for the slaves.


