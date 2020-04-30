#include "nsq.h"

const static char *NEW_LINE   = "\n";
const static char *NEW_SPACE  = " ";
const static int MAX_BUF_SIZE = 4096;
const static int MIN_BUF_SIZE = 2048;
const static int BUF_DELTER   = 2;

void *nsq_buf_malloc(size_t buf_size, size_t n, size_t l)
{
    if (buf_size - n >= MIN_BUF_SIZE || buf_size - l >= MIN_BUF_SIZE) {
        return NULL;
    }
    
    void *buf = NULL;
    while (1) {
        if (buf_size - n < MIN_BUF_SIZE || buf_size - l < MIN_BUF_SIZE) {
            buf_size *= BUF_DELTER;
            continue;
        }
        buf = malloc(buf_size * sizeof(void));
        assert(NULL != buf);
        break;
    }
    
    return buf;
}

void nsq_buffer_add(nsqBuf *buf, const char *name, const nsqCmdParams params[], size_t psize, const char *body)
{
    size_t buf_size = MAX_BUF_SIZE;
    void *b = malloc(buf_size * sizeof(void));
    void *nb = NULL;
    int v = 0;
    assert(NULL != b);
    size_t n = 0;
    size_t l = 0;

    l = strlen(name);
    memcpy(b, name, l);
    n += l;

    if (NULL != params) {
        for (int i = 0; i < psize; i++) {
            l = 0;
            memcpy(b+n, NEW_SPACE, 1);
            n++;
            nb = nsq_buf_malloc(buf_size, n, l);
            if (NULL != nb) {
                memcpy(nb, b, n);
                free(b);
                b = nb;
            }
            switch (params[i].t) {
                case NSQ_PARAM_TYPE_INT:
                    v = *((int *)params[i].v);
                    do {
                        v/=10;
                        l++;
                    } while(v!=0);
                    sprintf(b+n, "%d", *((int *)params[i].v));
                    break;
                case NSQ_PARAM_TYPE_CHAR:
                    l = strlen(params[i].v);
                    memcpy(b+n, params[i].v, l);
                    break;
            }
            n += l;
        }
    }
    memcpy(b+n, NEW_LINE, 1);
    n++;

    if (NULL != body) {
        l = 0;
        char byte[4] = "\0";
        uint32_t v = sizeof(body);
        byte[0] = (uint8_t)v >> 24;
        byte[1] = (uint8_t)v >> 16;
        byte[2] = (uint8_t)v >> 8;
        byte[3] = (uint8_t)v;
        memcpy(b+n, byte, 4);
        n += 4;
        l = strlen(body);
        nb = nsq_buf_malloc(buf_size, n, l);
        if (NULL != nb) {
            memcpy(nb, b, n);
            free(b);
            b = nb;
        }

        memcpy(b+n, body, l);
        n += l;
    }
    
    buffer_add(buf, b, n);
}

void nsq_subscribe(nsqBuf *buf, const char *topic, const char *channel)
{
    const char *name = "SUB";
    const nsqCmdParams params[2] = {
        {(void *)topic, NSQ_PARAM_TYPE_CHAR},
        {(void *)channel, NSQ_PARAM_TYPE_CHAR},
    };
    nsq_buffer_add(buf, name, params, 2, NULL);
}

void nsq_ready(nsqBuf *buf, int count)
{
    const char *name = "RDY";
    const nsqCmdParams params[1] = {
        {&count, NSQ_PARAM_TYPE_INT},
    };
    nsq_buffer_add(buf, name, params, 1, NULL);
}

void nsq_finish(nsqBuf *buf, const char *id)
{
    const char *name = "FIN";
    const nsqCmdParams params[1] = {
        {(void *)id, NSQ_PARAM_TYPE_CHAR},
    };
    nsq_buffer_add(buf, name, params, 1, NULL);
}

void nsq_requeue(nsqBuf *buf, const char *id, int timeout_ms)
{
    const char *name = "REQ";
    const nsqCmdParams params[2] = {
        {(void *)id, NSQ_PARAM_TYPE_CHAR},
        {&timeout_ms, NSQ_PARAM_TYPE_INT},
    };
    nsq_buffer_add(buf, name, params, 2, NULL);
}

void nsq_nop(nsqBuf *buf)
{
    nsq_buffer_add(buf, "NOP", NULL, 0, NULL);
}

void nsq_publish(nsqBuf *buf, const char *topic, const char *body)
{
    const char *name = "PUB";
    const nsqCmdParams params[1] = {
        {(void *)topic, NSQ_PARAM_TYPE_CHAR},
    };
    nsq_buffer_add(buf, name, params, 1, body);
}

void nsq_mpublish(nsqBuf *buf, const char *topic, const char **body, const size_t bs)
{
    const char *name = "MPUB";
    const nsqCmdParams params[1] = {
        {(void *)topic, NSQ_PARAM_TYPE_CHAR},
    };

    size_t v = 4;
    for (int i = 0; i<bs; i++) {
        v += strlen(body[i])+4;
    }
    char *b = malloc(v * sizeof(char));
    assert(NULL != b);
    
    uint32_t n = sizeof(body)/sizeof(*body);

    char nbyte[4] = "\0";
    nbyte[0] = (uint8_t)n >> 24;
    nbyte[1] = (uint8_t)n >> 16;
    nbyte[2] = (uint8_t)n >> 8;
    nbyte[3] = (uint8_t)n;
    memcpy(b, nbyte, 4);

    size_t l = 0;
    for (int i = 0; i < n; i++) {
        l = strlen(body[i]);
        char byte[4] = "\0";
        byte[0] = (uint8_t)((int32_t)l >> 24);
        byte[1] = (uint8_t)((int32_t)l >> 16);
        byte[2] = (uint8_t)((int32_t)l >> 8);
        byte[3] = (uint8_t)((int32_t)l);
        memcpy(b+4, byte, 4);
        memcpy(b+4, body[i], l);
    }

    nsq_buffer_add(buf, name, params, 1, b);
}

void nsq_dpublish(nsqBuf *buf, const char *topic, const char *body, int defer_time_sec)
{
    const char *name = "DPUB";
    const nsqCmdParams params[2] = {
        {(void *)topic, NSQ_PARAM_TYPE_CHAR},
        {&defer_time_sec, NSQ_PARAM_TYPE_INT},
    };
    nsq_buffer_add(buf, name, params, 2, body);
}

void nsq_touch(nsqBuf *buf, const char *id)
{
    const char *name = "TOUCH";
    const nsqCmdParams params[1] = {
        {(void *)id, NSQ_PARAM_TYPE_CHAR},
    };
    nsq_buffer_add(buf, name, params, 1, NULL);
}

void nsq_cleanly_close_connection(nsqBuf *buf)
{
    const char *name = "CLS";
    nsq_buffer_add(buf, name, NULL, 0, NULL);
}

void nsq_auth(nsqBuf *buf, const char *secret)
{
    const char *name = "AUTH";
    nsq_buffer_add(buf, name, NULL, 0, secret);
}

//TODO: should handle object to json string
void nsq_identify(nsqBuf *buf, const char *json_body)
{
    const char *name = "IDENTIFY";
    nsq_buffer_add(buf, name, NULL, 1, json_body);
}
