# CS744

### Commands
Connection Setup  
0. `vim .ssh/config`
1. Add ssh connection details as
```
Host <host>
  HostName <host name>
  User <user>
  Port <port>
 
Host *
    AddKeysToAgent yes
    IdentityFile  <path to private key>
```
2. `bash`
3. `ssh <host>`
4. `sudo su`

To list all the running java processes  
`jsp`
