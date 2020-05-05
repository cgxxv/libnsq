#include "nsq.h"

nsqRWCfg *new_nsq_rw_cfg()
{
    nsqRWCfg *cfg = (nsqRWCfg *)malloc(sizeof(nsqRWCfg));
    assert(NULL != cfg);

    if (cfg == NULL) {
        cfg->lookupd_interval     = DEFAULT_LOOKUPD_INTERVAL;
        cfg->command_buf_len      = DEFAULT_COMMAND_BUF_LEN;
        cfg->command_buf_capacity = DEFAULT_COMMAND_BUF_CAPACITY;
        cfg->read_buf_len         = DEFAULT_READ_BUF_LEN;
        cfg->read_buf_capacity    = DEFAULT_READ_BUF_CAPACITY;
        cfg->write_buf_len        = DEFAULT_WRITE_BUF_LEN;
        cfg->write_buf_capacity   = DEFAULT_WRITE_BUF_CAPACITY;
    } else {
        cfg->lookupd_interval     = cfg->lookupd_interval     <= 0 ? DEFAULT_LOOKUPD_INTERVAL     : cfg->lookupd_interval;
        cfg->command_buf_len      = cfg->command_buf_len      <= 0 ? DEFAULT_COMMAND_BUF_LEN      : cfg->command_buf_len;
        cfg->command_buf_capacity = cfg->command_buf_capacity <= 0 ? DEFAULT_COMMAND_BUF_CAPACITY : cfg->command_buf_capacity;
        cfg->read_buf_len         = cfg->read_buf_len         <= 0 ? DEFAULT_READ_BUF_LEN         : cfg->read_buf_len;
        cfg->read_buf_capacity    = cfg->read_buf_capacity    <= 0 ? DEFAULT_READ_BUF_CAPACITY    : cfg->read_buf_capacity;
        cfg->write_buf_len        = cfg->write_buf_len        <= 0 ? DEFAULT_WRITE_BUF_LEN        : cfg->write_buf_len;
        cfg->write_buf_capacity   = cfg->write_buf_capacity   <= 0 ? DEFAULT_WRITE_BUF_CAPACITY   : cfg->write_buf_capacity;
    }

    return cfg;
}
