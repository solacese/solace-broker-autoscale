"""Managed payment handover example. Its SQLite ledger has no external financial effect.

Install the root project with service/compile extras and adapters/python[smf]. Set
SOLACE_ASSIGNMENT_API_KEY and SOLACE_AUTOSCALE_CLIENT_PASSWORD. Initial brokers must
have the same application credential as provisioned brokers. Each publisher process
owns its own persistent outbox file. Never run two publishers against the same file.
"""
from __future__ import annotations

import argparse
import json
import os
import sqlite3
import time

from solace_autoscale_client import KeyRouter, Resolver
from solace_autoscale_client.managed_smf import ManagedConsumers, SmfConnections
from solace_autoscale_client.outbox import DurableOutbox


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('mode', choices=['consume', 'publish'])
    parser.add_argument('--assignment-url', default='http://127.0.0.1:8099')
    parser.add_argument('--shard', default='payments')
    parser.add_argument('--partitions', type=int, default=128)
    parser.add_argument('--outbox', default='publisher.outbox.db')
    parser.add_argument('--ledger', default='payments.ledger.db')
    parser.add_argument('--key', default='account-123')
    parser.add_argument('--event-id', help='Stable ID from the incoming request, reused on retries')
    parser.add_argument('--payload', default='{"amount_minor":100,"currency":"EUR"}')
    args = parser.parse_args()
    resolver = Resolver(args.assignment_url, api_key=os.environ['SOLACE_ASSIGNMENT_API_KEY'])
    pool = SmfConnections(lambda broker_id: (
        os.environ.get('SOLACE_AUTOSCALE_CLIENT_USERNAME', 'autoscale-app'),
        os.environ['SOLACE_AUTOSCALE_CLIENT_PASSWORD'],
    ))
    try:
        if args.mode == 'publish':
            if not args.event_id:
                parser.error('publish requires --event-id (use the original ID when retrying)')
            payload = json.dumps(json.loads(args.payload)).encode()
            router = KeyRouter(resolver, args.shard, 'example-publisher', partitions=args.partitions)
            outbox = DurableOutbox(args.outbox, router)
            try:
                outbox.enqueue(args.key, args.event_id, payload)
                while outbox.pending():
                    outbox.flush(pool.send)
                    if outbox.pending():
                        time.sleep(1)
            finally:
                outbox.close()
        else:
            # A separate connection per callback allows partition workers to call concurrently.
            with sqlite3.connect(args.ledger) as db:
                db.execute('PRAGMA journal_mode=WAL')
                db.execute('CREATE TABLE IF NOT EXISTS payments '
                           '(event_id TEXT PRIMARY KEY, payload TEXT NOT NULL)')

            def handle(event_id: str, payload: object) -> None:
                with sqlite3.connect(args.ledger, timeout=30) as db:
                    db.execute('PRAGMA synchronous=FULL')
                    db.execute('INSERT OR IGNORE INTO payments VALUES (?, ?)',
                               (event_id, json.dumps(payload, sort_keys=True)))
                    # The insert IS the example business effect and deduplication, in one commit.
                    # Real systems commit their event ID and business changes in the SAME transaction.

            consumers = ManagedConsumers(resolver, args.shard, pool, handle)
            try:
                while True:
                    try:
                        consumers.sync()
                    except Exception as exc:
                        print(f'Discovery retry: {type(exc).__name__}', flush=True)
                    time.sleep(2)
            finally:
                consumers.close()
    finally:
        pool.close()


if __name__ == '__main__':
    main()
