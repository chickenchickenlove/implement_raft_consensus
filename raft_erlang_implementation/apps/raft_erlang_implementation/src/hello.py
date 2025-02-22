# if (
#         currentTerm == m.term and
#         votedFor in [None, peer] and
#         (
#                 m.lastLogTerm > logTerm(len(log)) or
#                 (
#                         m.lastLogTerm == logTerm(len(log)) and
#                         m.lastLogIndex >= len(log)
#                 )
#         )
# ):