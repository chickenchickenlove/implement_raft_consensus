# Study Level RAFT Consensus algorithm with erlang
This code is an implementation of the RAFT consensus algorithm written for learning purposes.
A single erlang actor process represents a single RAFT node.
So, If you want to create a RAFT cluster with 5 nodes, you need to spawn 5 erlang actor processes.

In this code, each RAFT node communicates with other nodes via `erlang message passing`.
This means that no TCP connection is involved. 

This RAFT implementation supports this features below.
- RAFT Basic.
  - Log Replication (Append Entries RPC)
  - Leader Election (Request Vote RPC)
- RAFT Advanced
  - Joint Consensus. 

This RAFT implementation don't support this features below. 
- RAFT Advanced.
  - Install Snapshot.
  - Log Compaction.


## Reference
- Reference1 : https://raft.github.io/raft.pdf
- Reference2 : https://web.stanford.edu/~ouster/cgi-bin/papers/OngaroPhD.pdf
- pseudo code : https://github.com/ongardie/raft-pseudocode

## How to Test
- There are 98 test cases. some test cases may be flaky sometimes.
- Test cases in eunit can cover this action
  - RAFT basic.
  - Split Brain
  - Split Vote
  - Joint Consensus

```shell
$ rebar3 eunit
===> Verifying dependencies...
===> Analyzing applications...
===> Compiling raft_erlang_implementation
===> Performing EUnit tests...
..................................................................................................
Finished in 27.326 seconds
98 tests, 0 failures
```