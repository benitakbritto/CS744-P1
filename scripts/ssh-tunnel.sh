# UI
# node 0
ssh -L 9090:10.10.1.1:8080 hrpatel5@node0

# node 1
ssh -L 9091:10.10.1.2:8081 hrpatel5@node1

# node 2
ssh -L 9092:10.10.1.3:8081 hrpatel5@node2

# system metrics
ssh -L 5050:10.10.1.1:4040 hrpatel5@node0

# system metrics - history
ssh -L 18080:10.10.1.1:18080 hrpatel5@node0
