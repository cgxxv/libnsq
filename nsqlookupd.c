#include "nsq.h"

int nsq_lookupd_connect_producer(nsqLookupdEndpoint *lookupd, const int count, const char *topic,
    httpClient *httpc, void *arg)
{
    nsqLookupdEndpoint *nsqlookupd_endpoint;
    httpRequest *req;
    int i = 0, idx;
    char buf[256];

    idx = rand() % count;

    _DEBUG("%s: lookupd %p (chose %d), topic %s, httpClient %p", __FUNCTION__, lookupd, idx, topic, httpc);

    LL_FOREACH(lookupd, nsqlookupd_endpoint) {
        if (i == idx) {
            sprintf(buf, "http://%s:%d/lookup?topic=%s", nsqlookupd_endpoint->address,
                nsqlookupd_endpoint->port, topic);
            req = new_http_request(buf, nsq_lookupd_request_cb, arg);
            http_client_get(httpc, req);
            break;
        }
    }
    
    return idx;
}

void nsq_lookupd_request_cb(httpRequest *req, httpResponse *resp, void *arg)
{
    nsqio *nio = (nsqio *)arg;
    nsqdConn *conn;
    nsq_json_t *jsobj, *producers, *producer, *broadcast_address_obj, *tcp_port_obj;
    nsq_json_tokener_t *jstok;
    const char *broadcast_address;
    int i, found, tcp_port, rc = 0;

    _DEBUG("%s: status_code %d, body %.*s", __FUNCTION__, resp->status_code,
        (int)BUFFER_HAS_DATA(resp->data), resp->data->data);

    if (resp->status_code != 200) {
        free_http_response(resp);
        free_http_request(req);
        return;
    }

    jstok = nsq_json_tokener_new();
    jsobj = nsq_json_loadb(resp->data->data, (nsq_json_size_t)BUFFER_HAS_DATA(resp->data), 0, jstok);
    if (!jsobj) {
        _DEBUG("%s: error parsing JSON", __FUNCTION__);
        nsq_json_tokener_free(jstok);
        return;
    }

    nsq_json_object_get(jsobj, "producers", &producers);
    if (!producers) {
        _DEBUG("%s: error getting 'producers' key", __FUNCTION__);
        nsq_json_decref(jsobj);
        nsq_json_tokener_free(jstok);
        return;
    }

    _DEBUG("%s: num producers %ld", __FUNCTION__, (long)nsq_json_array_length(producers));
    for (i = 0; i < nsq_json_array_length(producers); i++) {
        producer = nsq_json_array_get(producers, i);
        nsq_json_object_get(producer, "broadcast_address", &broadcast_address_obj);
        nsq_json_object_get(producer, "tcp_port", &tcp_port_obj);

        broadcast_address = nsq_json_string_value(broadcast_address_obj);
        tcp_port = nsq_json_int_value(tcp_port_obj);

        _DEBUG("%s: broadcast_address %s, port %d", __FUNCTION__, broadcast_address, tcp_port);

        found = 0;
        LL_FOREACH(nio->conns, conn) {
            if (strcmp(conn->bs->address, broadcast_address) == 0
                && conn->bs->port == tcp_port) {
                found = 1;
                break;
            }
        }

        if (!found) {
            if (nio->mode == NSQ_LOOKUPD_MODE_READ) {
                rc = nsq_reader_connect_to_nsqd(nio, broadcast_address, tcp_port);
            } else if (nio->mode == NSQ_LOOKUPD_MODE_WRITE) {
                rc = nsq_writer_connect_to_nsqd(nio, broadcast_address, tcp_port);
            }
            if (rc <= 0) {
                _DEBUG("\033[31m%s:%d Error connected to (0:read,1:write %d) nsqd\033[0m", __FILE__, __LINE__, nio->mode);
            } else {
                _DEBUG("\033[32m%s:%d Success connected to (0:read,1:write %d) nsqd\033[0m", __FILE__, __LINE__, nio->mode);
            }
        }
    }

    nsq_json_decref(jsobj);
    nsq_json_tokener_free(jstok);

    free_http_response(resp);
    free_http_request(req);
}

nsqLookupdEndpoint *new_nsqlookupd_endpoint(const char *address, int port)
{
    nsqLookupdEndpoint *nsqlookupd_endpoint;

    nsqlookupd_endpoint = (nsqLookupdEndpoint *)malloc(sizeof(nsqLookupdEndpoint));
    nsqlookupd_endpoint->address = strdup(address);
    nsqlookupd_endpoint->port = port;
    nsqlookupd_endpoint->next = NULL;

    return nsqlookupd_endpoint;
}

void free_nsqlookupd_endpoint(nsqLookupdEndpoint *nsqlookupd_endpoint)
{
    if (nsqlookupd_endpoint) {
        free(nsqlookupd_endpoint->address);
        free(nsqlookupd_endpoint);
    }
}
