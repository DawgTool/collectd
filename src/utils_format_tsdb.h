/**
 * collectd - src/utils_format_tsdb.h
 * Copyright (C) 2009       Florian octo Forster
 * Copyright (C) 2015       Dallin Young
 *
 * Permission is hereby granted, free of charge, to any person obtaining a
 * copy of this software and associated documentation files (the "Software"),
 * to deal in the Software without restriction, including without limitation
 * the rights to use, copy, modify, merge, publish, distribute, sublicense,
 * and/or sell copies of the Software, and to permit persons to whom the
 * Software is furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
 * DEALINGS IN THE SOFTWARE.
 *
 * Authors:
 *   Florian octo Forster <octo at collectd.org>
 *   Dallin Young <dallin.young@gmail.com>
 **/

#ifndef UTILS_FORMAT_TSDB_H
#define UTILS_FORMAT_TSDB_H 1

#define UTILS_FORMAT_TSDB_TYPE_TAGS 0
#define UTILS_FORMAT_TSDB_TYPE_GRAPHITE 1

#define UTILS_FORMAT_TSDB_SEND_PUT  0
#define UTILS_FORMAT_TSDB_SEND_HTTP 1

#include "collectd.h"
#include "plugin.h"

struct curl_string_s
{
  char *ptr;
  size_t len;
};
typedef struct curl_string_s curl_string_t;

struct tsdb_tag_s
{
  char tag[DATA_MAX_NAME_LEN];
  char name[DATA_MAX_NAME_LEN];
};
typedef struct tsdb_tag_s tsdb_tag_t;

struct tsdb_config_s
{
  char *node;
  char *location;
  char *user;
  char *pass;
  char *credentials;
  _Bool verify_peer;
  _Bool verify_host;
  char *cacert;
  char *capath;
  char *clientkey;
  char *clientcert;
  char *clientkeypass;
  long sslversion;
  _Bool store_rates;
  _Bool append_ds;
  char *replace_char;
  int send_format;
  int data_format;
  int tag_num;
  tsdb_tag_t *tags[32];
};
typedef struct tsdb_config_s tsdb_config_t;

struct tsdb_rec_s
{
  char metric[10 * DATA_MAX_NAME_LEN];
  long timestamp;
  int tag_num;
  char value[512];
  tsdb_tag_t tags[32];
};
typedef struct tsdb_rec_s tsdb_rec_t;

int format_tsdb_initialize (char *buffer,
			    size_t * ret_buffer_fill,
			    size_t * ret_buffer_free);
int format_tsdb_value_list (char *buffer, size_t * ret_buffer_fill,
			    size_t * ret_buffer_free, const data_set_t * ds,
			    const value_list_t * vl,
			    const tsdb_config_t * config);
int format_tsdb_finalize (char *buffer, size_t * ret_buffer_fill,
			  size_t * ret_buffer_free);

#endif /* UTILS_FORMAT_TSDB_H */

/* vim: set fdm=marker sw=2 ts=2 tw=78 sts=2 et : */
