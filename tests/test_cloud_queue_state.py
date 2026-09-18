"""Deletion observations must cover every endpoint page and reject incomplete replies."""
import httpx
import pytest

from solace_autoscale.actuator.solace_cloud import SempConnection, SolaceCloudClient


def cloud_with_responses(handler):
    cloud = SolaceCloudClient('test-token', semp_connections={
        'svc': SempConnection('https://broker.example', 'test', 'test'),
    })
    cloud._semp_client.close()
    cloud._semp_client = httpx.Client(transport=httpx.MockTransport(handler))
    return cloud


def payload(request):
    path = request.url.path
    if path.endswith('/msgVpns/orders'):
        return {'data': {'msgSpoolMsgCount': 0, 'msgSpoolUsage': 0}}
    if path.endswith('/clients'):
        return {'data': [], 'meta': {'count': 0}}
    if path.endswith('/topicEndpoints'):
        return {'data': [], 'collections': [], 'meta': {'count': 0}}
    # The second queue page contains pending messages; the first is empty.
    second = 'cursor=next' in str(request.url)
    body = {'data': [{'queueName': 'q2' if second else 'q1'}],
            'collections': [{'msgs': {'count': 7 if second else 0}, 'txFlows': {'count': 0}}],
            'meta': {'count': 2}}
    if not second:
        body['meta']['paging'] = {'nextPageUri':
            'https://broker.example/SEMP/v2/monitor/msgVpns/orders/queues?cursor=next'}
    return body


def test_second_page_messages_prevent_empty_result():
    calls = []
    def handler(req):
        calls.append(str(req.url))
        return httpx.Response(200, json=payload(req))
    c = cloud_with_responses(handler)
    try:
        state = c.queue_state('svc', 'orders')
        assert state['total_msgs_spooled'] >= 7
        assert any('cursor=next' in u for u in calls)
        assert all(u.startswith('https://broker.example/') for u in calls)
    finally:
        c.close()


def test_missing_response_data_is_not_an_empty_broker():
    c = cloud_with_responses(lambda req: httpx.Response(200, json={}))
    try:
        with pytest.raises(ValueError):
            c.queue_state('svc', 'orders')
    finally:
        c.close()


def test_missing_pagination_link_with_unread_endpoints_is_rejected():
    def handler(req):
        body = payload(req)
        body.get('meta', {}).pop('paging', None)
        return httpx.Response(200, json=body)
    c = cloud_with_responses(handler)
    try:
        with pytest.raises(ValueError, match='incomplete'):
            c.queue_state('svc', 'orders')
    finally:
        c.close()


def test_spool_bytes_and_connected_clients_are_preserved():
    def handler(req):
        body = payload(req)
        if req.url.path.endswith('/msgVpns/orders'):
            body['data']['msgSpoolUsage'] = 45
        if req.url.path.endswith('/clients'):
            body['meta']['count'] = 2
        return httpx.Response(200, json=body)
    c = cloud_with_responses(handler)
    try:
        state = c.queue_state('svc', 'orders')
        assert state['spooled_bytes'] == 45
        assert state['active_flows'] == 2
    finally:
        c.close()


def test_missing_child_flow_count_is_fetched_from_flow_collection():
    def handler(req):
        if req.url.path.endswith('/txFlows'):
            return httpx.Response(200, json={'data': [], 'meta': {'count': 3}})
        body = payload(req)
        for child in body.get('collections', []):
            child['txFlows'] = {}
        return httpx.Response(200, json=body)
    c = cloud_with_responses(handler)
    try:
        assert c.queue_state('svc', 'orders')['active_flows'] == 6
    finally:
        c.close()
