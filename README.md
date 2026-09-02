# solace-broker-autoscale

**How many Solace PubSub+ Cloud brokers does a workload need, and when?**

`solace-broker-autoscale` works that out from capacity numbers you measure on your own brokers. It
gives you a recommendation *and* the reason behind it, never just a number. It only advises by
default; letting it scale for you is a separate, deliberate switch.

Vertical scaling on Solace Cloud has a ceiling: once you are on the largest practical service class
and traffic keeps growing, there is no bigger broker to buy. This is the horizontal path.

🔗 **[Live overview and examples](https://solacese.github.io/solace-broker-autoscale/)**

> Community project. Not a supported Solace product. No warranty. Apache 2.0.
> No measured numbers, prices, or customer data ship with the tool. You supply your own.

---

## Install

Python 3.11+.

```bash
pip install -e '.[compile]'
```

The command installed is `solace-autoscale`.

## Use it

You need two things: a config (start from the bundled `config.example.yaml`) and a metrics file, a
JSON snapshot of your broker's load (format in [`docs/metrics.md`](docs/metrics.md)).

```bash
# what does this workload need right now?
solace-autoscale recommend --config config.example.yaml --metrics metrics.json

# what would it need if traffic doubled or quadrupled?
solace-autoscale whatif --config config.example.yaml --metrics metrics.json --multipliers 1,2,4
```

Out of the box these run against a bundled sample model (`models/synthetic-v0.json`) with obviously
fake capacity numbers, flagged in every report and **blocked from making any real change** so you
can see the output shape safely. Nothing acts on fake data.

To use your own numbers, measure your brokers into a workbook (see
[`docs/benchmark.md`](docs/benchmark.md)), compile it, and point your config at the result:

```bash
solace-autoscale compile --workbook performance.xlsx \
    --service-classes models/service-classes.json --out models/mymodel.json
# then set  capacity.model: models/mymodel.json  in your config
```

## All commands

| Command | Does |
|---|---|
| `recommend` | How many brokers a workload needs, with the reason: which limit binds, how full each one is, and the cost. |
| `whatif` | The same, projected under load multipliers, so you see where the ceiling bites before it does. |
| `compile` | Turn a measured workbook into a versioned model file. Run once when your numbers change. |
| `monitor` | Watch a live broker over time and record how accurate past advice was. |
| `simulate` | Stress-test the model across many message sizes and fan-outs. |
| `accuracy` | Predicted vs. what actually happened, per limit. |
| `shard-advise` | Suggest how to split traffic into groups, from an Event Portal export. |
| `serve` | Point clients at the right broker (per-protocol endpoints, sticky durable placement). |

## Settings

Everything lives in one YAML file; unknown keys are rejected and the defaults are cautious. Start
from [`config.example.yaml`](config.example.yaml). The ones you reach for first:

| Setting | Effect |
|---|---|
| `fleet.min_brokers` / `max_brokers` | Floor and ceiling on every recommendation. Hitting the ceiling is reported, never hidden. |
| `workload.delivery` / `bottleneck` | `direct`/`guaranteed`/`mixed`; force a limit or let it auto-detect the binding one. |
| `metrics.source` | Where numbers come from: `semp`, `prometheus`, `cloud-api`, or a `static` file. |
| `billing.per_broker_monthly` | Your own prices, to turn broker counts into monthly cost. None ship with the tool. |
| `actuation.mode` | `recommend` (default, advice only), `scale-up-only`, or `full`. |

Full reference: [`docs/configuration.md`](docs/configuration.md).

## Safety (off by default)

It cannot touch your brokers unless you turn scaling on, and even then it is heavily guarded:

- Defaults to advice only. In `recommend` mode the part that makes changes is never even created.
- A sample model or stale metrics **block all changes**.
- Every real change is written to an audit log first, honours a kill-switch file and rate limits,
  requires explicit confirmation, and refuses to delete a broker that still has traffic on it.
- TLS certificate checking is on by default; disabling it takes an explicit `--insecure` flag.

See [`docs/safety.md`](docs/safety.md).

## Documentation

- [`docs/architecture.md`](docs/architecture.md) - how the pieces fit together
- [`docs/capacity-model.md`](docs/capacity-model.md) - the model file and how to build it
- [`docs/configuration.md`](docs/configuration.md) - every setting
- [`docs/metrics.md`](docs/metrics.md) - where the numbers come from
- [`docs/client-integration.md`](docs/client-integration.md) - pointing clients at the right broker
- [`docs/safety.md`](docs/safety.md) - the guardrails
- [`docs/adr/`](docs/adr/) - the design decisions and why

## Develop

```bash
pip install -e '.[dev]'    # tests, linters, type checker, compiler
pytest                     # unit suite (live-broker tests are skipped unless you ask for them)
ruff check . && mypy src/solace_autoscale
```

Running the live-broker tests needs the extra clients and a local broker; see
[`tests/test_integration_broker.py`](tests/test_integration_broker.py) for the one-line Docker
command and `pytest -m integration`.

Contributions welcome. The decision engine is a pure function (numbers in, recommendation out, no
I/O) - please keep it that way.
