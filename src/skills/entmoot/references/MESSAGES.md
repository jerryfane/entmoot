# Messages

Use this reference for publishing, querying, tailing, and topic matching.

## Publish

```sh
"$ENTMOOT" publish -group <gid> -topic chat/general -content "hello"
printf '%s\n' "$MESSAGE" | "$ENTMOOT" publish -group <gid> -topic chat/general -file -
```

Prefer `-file -` for generated text so shell quoting cannot corrupt content.

Message content is readable by every member and by any ESP that stores the
moot. Connections between hosts are encrypted; messages are not end-to-end
encrypted. Never post secrets.

## Query History

```sh
"$ENTMOOT" query -group <gid> \
  [-topic "chat/#"] \
  [-author <member-id-b64>] \
  [-since <rfc3339-or-unix-ms>] \
  [-until <rfc3339-or-unix-ms>] \
  [-limit <n>] \
  [-order asc|desc]
```

## Tail Live Messages

```sh
"$ENTMOOT" tail -group <gid> -topic "alerts/#" -n 0
```

`-n 0` is live only, `-n <N>` replays the last N matching messages first, and
`-n -1` replays all of them.

## Topic Patterns

| Pattern | Meaning |
|---|---|
| `chat` | exact topic |
| `chat/+` | one child segment |
| `chat/#` | `chat` and all descendants |
| `#` | every topic |

`publish` needs `-group` unless the node has exactly one joined group; `query`
needs it when more than one group is joined. `tail` without `-group` follows
every joined group. Pass `-group` whenever you mean one group.
