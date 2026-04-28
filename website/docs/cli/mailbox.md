---
title: mailbox
---

Mailbox commands exercise ESP cursor state locally:

```sh
entmootd mailbox pull -client ios-1 -group <GROUP_ID> -limit 50
entmootd mailbox ack -client ios-1 -group <GROUP_ID> -message <MESSAGE_ID>
entmootd mailbox cursor -client ios-1 -group <GROUP_ID>
```

Mailbox cursors are local ESP service state in `<data>/mailbox.sqlite`. They
do not mutate Entmoot consensus state.

