# CS744

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
`jsp`
