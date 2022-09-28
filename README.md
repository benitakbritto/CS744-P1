# CS744-P1

Project Description can be found [here](https://pages.cs.wisc.edu/~shivaram/cs744-fa22/assignment1.html)

### Commands
Connection Setup  
1. `vim .ssh/config`
2. Add ssh connection details as

```
Host <host>
  HostName <host name>
  User <user>
  Port <port>
 
Host *
    AddKeysToAgent yes
    IdentityFile  <path to private key>
```

3. `bash`
4. `ssh <host>`
5. `sudo su`

To list all the running java processes  
`jps`

To view current status of the HDFS cluster:
1. `ssh -L 9870:10.10.1.1:9870 <user>@<host>`
2. On a browser, go to URL `localhost:9870/dfshealth.html`

To view currently running Spark job(s):
1. `ssh -L 4040:10.10.1.1:4040 <user>@<host>`
2. On a browser, go to URL `localhost:4040`

To view job(s) history on the Spark History Server:
1. `ssh -L 18080:10.10.1.1:18080 <user>@<host>`
2. On a browser, go to URL `localhost:18080`

## Codebase specific commands 
### Part 2
To run part 2:
1. `sudo su`
2. `spark-submit --master spark://10.10.1.1:7077 part2/sort.py`

OR

Run the entire codebase


### Part 3
To run part 3:
1. `sudo su`
2. `spark-submit --master spark://10.10.1.1:7077 part3/pagerank.py --iterations <num> --partitions <num> --persist <"Memory_Only"/"Memory_And_Disk"> --out_partitions <num>`

OR

Run the entire codebase

### Running the entire codebase
1. `sudo su`
2. `chmod 777 run.sh`
3. `./run.sh`

### To clean up the output
1. `sudo su`
2. `chmod 777 clean_up.sh`
3. `./clean_up.sh`

### To utilize our test scripts
1. `sudo su`
2. `cd scripts`
3. `chmod 777 <filename>.sh`
4. `./<filename>.sh`

# Monitoring
1. We use [Ganglia](http://ganglia.info/) for acquiring the CPU, Network I/O and Memory usage for each node in the cluster.
2. For other metric wrt the job, we use Spark's History Server.