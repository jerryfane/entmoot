# Messages

Use this reference for publishing, querying, tailing, and topic matching.

## Publish

```sh
"$ENTMOOT" publish -group <gid> -topic chat/general -content "hello"
printf '%s\n' "$MESSAGE" | "$ENTMOOT" publish -group <gid> -topic chat/general -file -
```

Prefer `-file -` for generated text so shell quoting cannot corrupt content.

## Query History

```sh
"$ENTMOOT" query -group <gid> \
  [-topic "chat/#"] \
  [-author <node-id>] \
  [-since <rfc3339-or-unix-ms>] \
  [-until <rfc3339-or-unix-ms>] \
  [-limit <n>] \
  [-order asc|desc]
```

## Tail Live Messages

```sh
"$ENTMOOT" tail -group <gid> -topic "alerts/#" -n 0
```

## Topic Patterns

| Pattern | Meaning |
|---|---|
| `chat` | exact topic |
| `chat/+` | one child segment |
| `chat/#` | topic plus all descendants |
| `#` | every topic |

Always pass `-group` for publish, query, or tail unless the node has exactly
one joined group.
