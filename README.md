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

To view currently running Spark job(s):
1. `ssh -L 4040:10.10.1.1:4040 <user>@<host>`
2. On a browser, go to URL `localhost:4040`

To view job(s) history on the Spark History Server:
1. `ssh -L 18080:10.10.1.1:18080 <user>@<host>`
2. On a browser, go to URL `localhost:18080`

## Part 2
To run part 2:
1. `sudo su`
2. `spark-submit --master spark://10.10.1.1:7077 part1/sort.py`


## Part 3
To run part 3:
1. `sudo su`
2. `spark-submit --master spark://10.10.1.1:7077 part2/pagerank.py`
