# Smart shim rule spec

The smart shim decides, per message, which broker a message goes to, what partition key to stamp on
it, and what address to publish it to. Those decisions come from an ordered list of rules. The rules
are pure data: this document defines their portable format so a rule set authored and tested in the
Python reference engine can be shipped to an adapter in any language and evaluated identically.

The Python engine round-trips this format with `to_spec(plan)` and `from_spec(dict)` in
`solace_autoscale.dispatch`. The format is the same shape the config file already uses, plus a
`version` and a `default_broker`.

## Shape

```json
{
  "version": 1,
  "default_broker": "broker-default",
  "rules": [
    {
      "name": "vip-orders",
      "when": {
        "topic": "orders/>",
        "payload": [
          { "path": "priority", "op": "in", "value": ["high", "urgent"] }
        ]
      },
      "route": { "broker": "broker-vip", "key": "vip.{region}", "topic": "vip/{topic}" }
    },
    {
      "name": "big-orders",
      "when": {
        "topic": "orders/>",
        "payload": [ { "path": "amount", "op": "gt", "value": 1000 } ]
      },
      "route": { "broker": "broker-big", "key": "big.{region}" }
    },
    {
      "name": "telemetry",
      "when": { "topic": "telemetry/>" },
      "route": { "broker": "broker-bulk" }
    }
  ]
}
```

### Fields

- `version` (int): spec version. Bump when the shape changes in a way adapters must gate on. Current
  value is `1`.
- `default_broker` (string or null): where a message goes when no rule matches. If null and no rule
  matches, the message cannot be placed and the engine raises rather than guessing.
- `rules` (array): evaluated in order, first match wins.
  - `name` (string): the rule's name, echoed back on the decision so routing is traceable. The
    default fallback reports the name `default`.
  - `when.topic` (string): a Solace topic pattern (see wildcards below). Defaults to `>` (match all)
    when omitted.
  - `when.payload` (array, optional): payload predicates, all of which must match (AND). Each is
    `{ "path": "<dotted.path>", "op": "<operator>", "value": <literal> }`. `path` reads a field from
    the decoded JSON payload by dotted path; omit it (or leave it empty) for whole-payload operators
    such as `raw_size_gt`. `value` is the comparison literal and may be omitted for operators that do
    not take one.
  - `route.broker` (string): the target broker name. The shim resolves the name to an endpoint; the
    spec never contains endpoints or credentials.
  - `route.key` (string, optional): a template for the partition/group key. Omit for no key.
  - `route.topic` (string, optional): a template to rewrite the publish address. Omit to publish on
    the original topic.

## Evaluation semantics an adapter must honour

Any adapter, in any language, must produce the same decision the reference engine would for the same
`(topic, payload)`:

1. **Topic wildcards (Solace):** `*` matches exactly one topic level; `>` matches the rest of the
   topic (one or more remaining levels). `orders/>` matches `orders/eu/new` but not `orders`.
2. **A rule fires when** the topic pattern matches AND every payload predicate matches. Zero
   predicates means topic-only matching.
3. **First match wins.** Rules are evaluated top to bottom; the first rule that fires decides the
   message. This is deliberately not round-robin, so the same message always routes the same way and
   the listener side can reconstruct a coherent per-key stream.
4. **No match falls back** to `default_broker` (decision name `default`, no key, original topic). If
   `default_broker` is null, the message cannot be placed.
5. **Templates** (`route.key`, `route.topic`) fill `{topic}` with the original topic and
   `{dotted.path}` with a field from the decoded payload. An absent or null field renders as the
   empty string. Runs of `.` left by an empty field are collapsed to a single `.` and leading and
   trailing `.` are trimmed, so `vip.{region}.{tier}` with a missing `tier` yields `vip.<region>`,
   not `vip.<region>.`.
6. **Invalid JSON payload:** payload predicates that read a path do not match (the field is absent),
   but whole-payload operators such as `raw_size_gt` and `raw_prefix` still apply to the raw bytes.

## Why a spec, not just Python

The reference engine is Python, which is the right home for authoring and testing rules. But the
shim runs inside customer applications, which are written in many languages, and it sits on the
message hot path. Shipping the rules as this portable spec lets each language have a thin native
adapter that reads the same rules and routes the same way, with Python staying the place rules are
written, versioned, and tested.
