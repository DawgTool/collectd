/**
 * collectd - src/write_tsdb.c
 * Copyright (C) 2015       Dallin Young
 * Code 'borrowed' from src/write_http.c
 * Copyright (C) 2009       Paul Sadauskas
 * Copyright (C) 2009       Doug MacEachern
 * Copyright (C) 2007-2014  Florian octo Forster
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the
 * Free Software Foundation; only version 2 of the License is applicable.
 *
 * This program is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License along
 * with this program; if not, write to the Free Software Foundation, Inc.,
 * 51 Franklin St, Fifth Floor, Boston, MA  02110-1301 USA
 *
 * Authors:
 *   Florian octo Forster <octo at collectd.org>
 *   Doug MacEachern <dougm@hyperic.com>
 *   Paul Sadauskas <psadauskas@gmail.com>
 *   Dallin Young <dallin.young@gmail.com>
 **/

#include "collectd.h"
#include "plugin.h"
#include "common.h"
#include "utils_cache.h"
#include "utils_format_tsdb.h"

#if HAVE_PTHREAD_H
# include <pthread.h>
#endif

#include <curl/curl.h>

#ifndef WT_DEFAULT_BUFFER_SIZE
# define WT_DEFAULT_BUFFER_SIZE 4096
#endif

/*
 * Private variables
 */
struct wt_callback_s
{
  tsdb_config_t config;
  CURL *curl;
  char curl_errbuf[CURL_ERROR_SIZE];

  char *send_buffer;
  size_t send_buffer_size;
  size_t send_buffer_free;
  size_t send_buffer_fill;
  cdtime_t send_buffer_init_time;

  pthread_mutex_t send_lock;
};
typedef struct wt_callback_s wt_callback_t;

static void
wt_reset_buffer (wt_callback_t * cb)	/* {{{ */
{
  DEBUG ("write_tsdb plugin : %30s : Starting", "wt_reset_buffer");
  memset (cb->send_buffer, 0, cb->send_buffer_size);
  cb->send_buffer_free = cb->send_buffer_size;
  cb->send_buffer_fill = 0;
  cb->send_buffer_init_time = cdtime ();

  if (cb->config.send_format == UTILS_FORMAT_TSDB_SEND_HTTP)
    {
      DEBUG ("write_tsdb plugin : %30s : format_tsdb_initialize",
	     "wt_reset_buffer");
      format_tsdb_initialize (cb->send_buffer,
			      &cb->send_buffer_fill, &cb->send_buffer_free);
    }
  DEBUG ("write_tsdb plugin : %30s : Ending", "wt_reset_buffer");
}				/* }}} wt_reset_buffer */

static int
curl_init_string (curl_string_t * str)	/* {{{ */
{
  DEBUG ("write_tsdb plugin : %30s : Starting", "curl_init_string");
  str->len = 0;
  str->ptr = malloc (str->len + 1);
  if (str->ptr == NULL)
    {
      ERROR ("write_tsdb plugin : %30s : malloc failed!", "curl_init_string");
      return (-1);
    }
  str->ptr[0] = '\0';

  DEBUG ("write_tsdb plugin : %30s : Ending", "curl_init_string");
  return (0);
}				/* }}} curl_init_string */

static size_t
curl_writefunc (void *contents, size_t size, size_t nmemb, void *userp)	/* {{{ */
{
  DEBUG ("write_tsdb plugin : %30s : Starting", "curl_writefunc");
  size_t realsize = size * nmemb;
  curl_string_t *str = (curl_string_t *) userp;

  str->ptr = realloc (str->ptr, str->len + realsize + 1);
  if (str->ptr == NULL)
    {
      ERROR ("write_tsdb plugin : %30s : malloc failed!", "curl_writefunc");
      return (-1);
    }

  memcpy (&(str->ptr[str->len]), contents, realsize);
  str->len += realsize;
  str->ptr[str->len] = 0;

  DEBUG ("write_tsdb plugin : %30s : Curl response buffer : %zd",
	 "curl_writefunc", str->len);

  DEBUG ("write_tsdb plugin : %30s : Ending", "curl_writefunc");
  return realsize;
}				/* }}} curl_writefunc */

static int
wt_send_buffer (wt_callback_t * cb)	/* {{{ */
{
  DEBUG ("write_tsdb plugin : %30s : Starting", "wt_send_buffer");

  int i;
  int j = 0;
  long curl_response;
  curl_string_t curl_body;
  curl_string_t curl_header;

  char sbuf[DATA_MAX_NAME_LEN + 1];
  memset (sbuf, 0, DATA_MAX_NAME_LEN + 1);

  int status = 0;

  DEBUG ("write_tsdb plugin : %30s : curl_easy_setopt"
	 " : CURLOPT_POSTFIELDS : %d", "wt_send_buffer", CURLOPT_POSTFIELDS);

  for (i = 0; i < strlen (cb->send_buffer);)
    {
      if (i + DATA_MAX_NAME_LEN > strlen (cb->send_buffer))
	j = strlen (cb->send_buffer) - i;
      else
	j = DATA_MAX_NAME_LEN;

      memcpy (sbuf, &cb->send_buffer[i], j);
      sbuf[j + 1] = '\0';

      DEBUG ("write_tsdb plugin : %30s : curl_easy_setopt"
	     " : send_buffer : %s", "wt_send_buffer", sbuf);

      if (i + DATA_MAX_NAME_LEN > strlen (cb->send_buffer))
	i = strlen (cb->send_buffer);
      else
	i = i + DATA_MAX_NAME_LEN;

      memset (sbuf, 0, DATA_MAX_NAME_LEN + 1);
    }

  status = curl_init_string (&curl_body);
  if (status != 0)
    {
      ERROR ("write_tsdb plugin : %30s : Can not allocate memory"
	     "for curl body", "wt_send_buffer");
      return (status);
    }

  status = curl_init_string (&curl_header);
  if (status != 0)
    {
      ERROR ("write_tsdb plugin : %30s : Can not allocate memory"
	     "for curl header", "wt_send_buffer");
      return (status);
    }

  curl_easy_setopt (cb->curl, CURLOPT_POSTFIELDS, cb->send_buffer);
  curl_easy_setopt (cb->curl, CURLOPT_WRITEFUNCTION, curl_writefunc);
  curl_easy_setopt (cb->curl, CURLOPT_WRITEHEADER, &curl_header);
  curl_easy_setopt (cb->curl, CURLOPT_WRITEDATA, &curl_body);
  status = curl_easy_perform (cb->curl);
  if (status != CURLE_OK)
    {
      ERROR ("write_tsdb plugin : %30s : curl_easy_perform failed with "
	     "status %i: %s", "wt_send_buffer", status, cb->curl_errbuf);
    }

  curl_easy_getinfo (cb->curl, CURLINFO_RESPONSE_CODE, &curl_response);
  switch (curl_response)
    {
    case 200:
    case 204:
    case 301:
      DEBUG ("write_tsdb plugin : %30s : Response code : %ld",
	     "wt_send_buffer", curl_response);
      DEBUG ("write_tsdb plugin : %30s : Response header : %s",
	     "wt_send_buffer", curl_header.ptr);
      INFO ("write_tsdb plugin : %30s : Response body : %s",
	    "wt_send_buffer", curl_body.ptr);
      break;
    case 400:
    case 404:
    case 405:
    case 406:
    case 408:
    case 413:
    case 500:
    case 501:
    case 503:
      ERROR ("write_tsdb plugin : %30s : Response code : %ld",
	     "wt_send_buffer", curl_response);
      ERROR ("write_tsdb plugin : %30s : Response header : %s",
	     "wt_send_buffer", curl_header.ptr);
      ERROR ("write_tsdb plugin : %30s : Response body : %s",
	     "wt_send_buffer", curl_body.ptr);
      status = -1;
      break;
    default:
      WARNING ("write_tsdb plugin : %30s : Response code : %ld",
	       "wt_send_buffer", curl_response);
      WARNING ("write_tsdb plugin : %30s : Response header : %s",
	       "wt_send_buffer", curl_header.ptr);
      WARNING ("write_tsdb plugin : %30s : Response body : %s",
	       "wt_send_buffer", curl_body.ptr);
      break;
    }

  sfree (curl_body.ptr);
  sfree (curl_header.ptr);

  DEBUG ("write_tsdb plugin : %30s : Ending", "wt_send_buffer");
  return (status);
}				/* }}} wt_send_buffer */

static int
wt_callback_init (wt_callback_t * cb)	/* {{{ */
{
  DEBUG ("write_tsdb plugin : %30s : Starting", "wt_callback_init");
  struct curl_slist *headers;

  if (cb->curl != NULL)
    return (0);

  cb->curl = curl_easy_init ();
  if (cb->curl == NULL)
    {
      ERROR ("curl plugin: curl_easy_init failed.");
      return (-1);
    }

  curl_easy_setopt (cb->curl, CURLOPT_NOSIGNAL, 1L);
  curl_easy_setopt (cb->curl, CURLOPT_USERAGENT, COLLECTD_USERAGENT);

  headers = NULL;
  headers = curl_slist_append (headers, "Accept:  */*");

  /* This needs to change to a RAW PORT 'put' */
  if (cb->config.send_format == UTILS_FORMAT_TSDB_SEND_HTTP)
    headers = curl_slist_append (headers, "Content-Type: application/json");
  else
    headers = curl_slist_append (headers, "Content-Type: text/plain");
  headers = curl_slist_append (headers, "Expect:");
  curl_easy_setopt (cb->curl, CURLOPT_HTTPHEADER, headers);

  curl_easy_setopt (cb->curl, CURLOPT_ERRORBUFFER, cb->curl_errbuf);
  curl_easy_setopt (cb->curl, CURLOPT_URL, cb->config.location);
  curl_easy_setopt (cb->curl, CURLOPT_FOLLOWLOCATION, 1L);
  curl_easy_setopt (cb->curl, CURLOPT_MAXREDIRS, 50L);

  if (cb->config.user != NULL)
    {
      size_t credentials_size;

      credentials_size = strlen (cb->config.user) + 2;
      if (cb->config.pass != NULL)
	credentials_size += strlen (cb->config.pass);

      cb->config.credentials = (char *) malloc (credentials_size);
      if (cb->config.credentials == NULL)
	{
	  ERROR ("curl plugin: malloc failed.");
	  return (-1);
	}

      ssnprintf (cb->config.credentials, credentials_size, "%s:%s",
		 cb->config.user,
		 (cb->config.pass == NULL) ? "" : cb->config.pass);
      curl_easy_setopt (cb->curl, CURLOPT_USERPWD, cb->config.credentials);
      curl_easy_setopt (cb->curl, CURLOPT_HTTPAUTH, CURLAUTH_ANY);
    }

  curl_easy_setopt (cb->curl, CURLOPT_SSL_VERIFYPEER,
		    (long) cb->config.verify_peer);
  curl_easy_setopt (cb->curl, CURLOPT_SSL_VERIFYHOST,
		    cb->config.verify_host ? 2L : 0L);
  curl_easy_setopt (cb->curl, CURLOPT_SSLVERSION, cb->config.sslversion);
  if (cb->config.cacert != NULL)
    curl_easy_setopt (cb->curl, CURLOPT_CAINFO, cb->config.cacert);
  if (cb->config.capath != NULL)
    curl_easy_setopt (cb->curl, CURLOPT_CAPATH, cb->config.capath);

  if (cb->config.clientkey != NULL && cb->config.clientcert != NULL)
    {
      curl_easy_setopt (cb->curl, CURLOPT_SSLKEY, cb->config.clientkey);
      curl_easy_setopt (cb->curl, CURLOPT_SSLCERT, cb->config.clientcert);

      if (cb->config.clientkeypass != NULL)
	curl_easy_setopt (cb->curl, CURLOPT_SSLKEYPASSWD,
			  cb->config.clientkeypass);
    }

  wt_reset_buffer (cb);

  DEBUG ("write_tsdb plugin : %30s : Ending", "wt_callback_init");
  return (0);
}				/* }}} int wt_callback_init */

static int
wt_flush_nolock (cdtime_t timeout, wt_callback_t * cb)	/* {{{ */
{
  DEBUG ("write_tsdb plugin : %30s : Starting", "wt_flush_nolock");
  int status;

  DEBUG ("write_tsdb plugin : %30s : wt_flush_nolock: timeout = %.3f; "
	 "send_buffer_fill = %zu;",
	 "wt_flush_nolock",
	 CDTIME_T_TO_DOUBLE (timeout), cb->send_buffer_fill);

  /* timeout == 0  => flush unconditionally */
  if (timeout > 0)
    {
      cdtime_t now;

      now = cdtime ();
      if ((cb->send_buffer_init_time + timeout) > now)
	return (0);
    }

  if (cb->config.send_format == UTILS_FORMAT_TSDB_SEND_PUT)
    {
      if (cb->send_buffer_fill <= 0)
	{
	  cb->send_buffer_init_time = cdtime ();
	  return (0);
	}

      status = wt_send_buffer (cb);
      wt_reset_buffer (cb);
    }
  else if (cb->config.send_format == UTILS_FORMAT_TSDB_SEND_HTTP)
    {
      if (cb->send_buffer_fill <= 2)
	{
	  cb->send_buffer_init_time = cdtime ();
	  return (0);
	}

      status = format_tsdb_finalize (cb->send_buffer,
				     &cb->send_buffer_fill,
				     &cb->send_buffer_free);
      if (status != 0)
	{
	  ERROR ("write_tsdb: %30s : wt_flush_nolock: "
		 "format_tsdb_finalize failed.", "wt_flush_nolock");
	  wt_reset_buffer (cb);
	  return (status);
	}

      status = wt_send_buffer (cb);
      wt_reset_buffer (cb);
    }
  else
    {
      ERROR ("write_tsdb: %30s : wt_flush_nolock: "
	     "Unknown format: %i", "wt_flush_nolock", cb->config.send_format);
      return (-1);
    }

  DEBUG ("write_tsdb plugin : %30s : Ending", "wt_flush_nolock");
  return (status);
}				/* }}} wt_flush_nolock */

static int
wt_flush (cdtime_t timeout,	/* {{{ */
	  const char *identifier __attribute__ ((unused)),
	  user_data_t * user_data)
{
  DEBUG ("write_tsdb plugin : %30s : Starting", "wt_flush");
  wt_callback_t *cb;
  int status;

  if (user_data == NULL)
    return (-EINVAL);

  cb = user_data->data;

  pthread_mutex_lock (&cb->send_lock);

  if (cb->curl == NULL)
    {
      status = wt_callback_init (cb);
      if (status != 0)
	{
	  ERROR ("write_tsdb plugin : wt_callback_init failed.");
	  pthread_mutex_unlock (&cb->send_lock);
	  return (-1);
	}
    }

  status = wt_flush_nolock (timeout, cb);
  pthread_mutex_unlock (&cb->send_lock);

  DEBUG ("write_tsdb plugin : %30s : Ending", "wt_flush");
  return (status);
}				/* }}} int wt_flush */

static void
wt_callback_free (void *data)	/* {{{ */
{
  DEBUG ("write_tsdb plugin : %30s : Starting", "wt_callback_free");
  wt_callback_t *cb;

  if (data == NULL)
    return;

  cb = data;

  wt_flush_nolock ( /* timeout = */ 0, cb);

  if (cb->curl != NULL)
    {
      curl_easy_cleanup (cb->curl);
      cb->curl = NULL;
    }
  sfree (cb->config.node);
  sfree (cb->config.location);
  sfree (cb->config.user);
  sfree (cb->config.pass);
  sfree (cb->config.credentials);
  sfree (cb->config.cacert);
  sfree (cb->config.capath);
  sfree (cb->config.clientkey);
  sfree (cb->config.clientcert);
  sfree (cb->config.clientkeypass);
  sfree (cb->config.replace_char);
  sfree (cb->send_buffer);

  sfree (cb);
  DEBUG ("write_tsdb plugin : %30s : Ending", "wt_callback_free");
}				/* }}} void wt_callback_free */

static int
wt_write_command (const data_set_t * ds, const value_list_t * vl,	/* {{{ */
		  wt_callback_t * cb)
{
  DEBUG ("write_tsdb plugin : %30s : Starting", "wt_write_command");
  char key[10 * DATA_MAX_NAME_LEN];
  char values[512];
  char command[1024];
  size_t command_len;

  int status;

  if (0 != strcmp (ds->type, vl->type))
    {
      ERROR ("write_tsdb plugin : DS type does not match " "value list type");
      return -1;
    }

  /* Copy the identifier to `key' and escape it. */
  status = FORMAT_VL (key, sizeof (key), vl);
  if (status != 0)
    {
      ERROR ("write_tsdb plugin : error with format_name");
      return (status);
    }
  escape_string (key, sizeof (key));

  /* Convert the values to an ASCII representation and put that into
   * `values'. */
  status =
    format_values (values, sizeof (values), ds, vl, cb->config.store_rates);
  if (status != 0)
    {
      ERROR ("write_tsdb plugin : error with " "wt_value_list_to_string");
      return (status);
    }

  command_len = (size_t) ssnprintf (command, sizeof (command),
				    "PUTVAL %s interval=%.3f %s\r\n",
				    key,
				    CDTIME_T_TO_DOUBLE (vl->interval),
				    values);
  if (command_len >= sizeof (command))
    {
      ERROR ("write_tsdb plugin : %30s : Command buffer too small: "
	     "Need %zu bytes.", "wt_write_command", command_len + 1);
      return (-1);
    }

  pthread_mutex_lock (&cb->send_lock);

  if (cb->curl == NULL)
    {
      status = wt_callback_init (cb);
      if (status != 0)
	{
	  ERROR ("write_tsdb plugin : %30s : wt_callback_init failed.",
		 "wt_write_command");
	  pthread_mutex_unlock (&cb->send_lock);
	  return (-1);
	}
    }

  if (command_len >= cb->send_buffer_free)
    {
      status = wt_flush_nolock ( /* timeout = */ 0, cb);
      if (status != 0)
	{
	  pthread_mutex_unlock (&cb->send_lock);
	  return (status);
	}
    }
  assert (command_len < cb->send_buffer_free);

  /* `command_len + 1' because `command_len' does not include the
   * trailing null byte. Neither does `send_buffer_fill'. */
  memcpy (cb->send_buffer + cb->send_buffer_fill, command, command_len + 1);
  cb->send_buffer_fill += command_len;
  cb->send_buffer_free -= command_len;

  DEBUG ("write_tsdb plugin : %30s : <%s> buffer %zu/%zu (%g%%) \"%s\"",
	 "wt_write_command", cb->config.node,
	 cb->send_buffer_fill, cb->send_buffer_size,
	 100.0 * ((double) cb->send_buffer_fill) /
	 ((double) cb->send_buffer_size), command);

  /* Check if we have enough space for this command. */
  pthread_mutex_unlock (&cb->send_lock);

  DEBUG ("write_tsdb plugin : %30s : Ending", "wt_write_command");
  return (0);
}				/* }}} int wt_write_command */

static int
wt_write_json (const data_set_t * ds, const value_list_t * vl,	/* {{{ */
	       wt_callback_t * cb)
{
  DEBUG ("write_tsdb plugin : %30s : Starting", "wt_write_json");
  int status;

  pthread_mutex_lock (&cb->send_lock);

  if (cb->curl == NULL)
    {
      status = wt_callback_init (cb);
      if (status != 0)
	{
	  ERROR ("write_tsdb plugin : wt_callback_init failed.");
	  pthread_mutex_unlock (&cb->send_lock);
	  return (-1);
	}
    }

  status = format_tsdb_value_list (cb->send_buffer,
				   &cb->send_buffer_fill,
				   &cb->send_buffer_free,
				   ds, vl, &cb->config);
  if (status == (-ENOMEM))
    {
      status = wt_flush_nolock ( /* timeout = */ 0, cb);
      if (status != 0)
	{
	  wt_reset_buffer (cb);
	  pthread_mutex_unlock (&cb->send_lock);
	  return (status);
	}

      status = format_tsdb_value_list (cb->send_buffer,
				       &cb->send_buffer_fill,
				       &cb->send_buffer_free,
				       ds, vl, &cb->config);
    }
  if (status != 0)
    {
      pthread_mutex_unlock (&cb->send_lock);
      return (status);
    }

  DEBUG ("write_tsdb plugin : %30s : <%s> buffer %zu/%zu (%g%%)",
	 "wt_write_json", cb->config.node,
	 cb->send_buffer_fill, cb->send_buffer_size,
	 100.0 * ((double) cb->send_buffer_fill) /
	 ((double) cb->send_buffer_size));

  /* Check if we have enough space for this command. */
  pthread_mutex_unlock (&cb->send_lock);

  DEBUG ("write_tsdb plugin : %30s : Ending", "wt_write_json");
  return (0);
}				/* }}} int wt_write_json */

static int
wt_write (const data_set_t * ds, const value_list_t * vl,	/* {{{ */
	  user_data_t * user_data)
{
  DEBUG ("write_tsdb plugin : %30s : Starting", "wt_write");
  wt_callback_t *cb;
  int status;

  if (user_data == NULL)
    return (-EINVAL);

  cb = user_data->data;

  if (cb->config.send_format == UTILS_FORMAT_TSDB_SEND_HTTP)
    status = wt_write_json (ds, vl, cb);
  else
    status = wt_write_command (ds, vl, cb);

  DEBUG ("write_tsdb plugin : %30s : Ending", "wt_write");
  return (status);
}				/* }}} int wt_write */

static int
config_set_format (wt_callback_t * cb,	/* {{{ */
		   oconfig_item_t * ci)
{
  DEBUG ("write_tsdb plugin : %30s : Starting", "config_set_format");
  char *value;

  if ((ci->values_num != 1) || (ci->values[0].type != OCONFIG_TYPE_STRING))
    {
      WARNING ("write_tsdb plugin : The `%s' config option "
	       "needs exactly one string argument.", ci->key);
      return (-1);
    }

  value = ci->values[0].value.string;
  if (strcasecmp ("SendFormat", ci->key) == 0)
    {
      if (strcasecmp ("PUT", value) == 0)
	cb->config.send_format = UTILS_FORMAT_TSDB_SEND_PUT;
      else if (strcasecmp ("HTTP", value) == 0)
	cb->config.send_format = UTILS_FORMAT_TSDB_SEND_HTTP;
      else
	{
	  ERROR ("write_tsdb plugin : Invalid send format string: %s", value);
	  return (-1);
	}
    }
  else if (strcasecmp ("DataFormat", ci->key) == 0)
    {
      if (strcasecmp ("GRAPHITE", value) == 0)
	cb->config.data_format = UTILS_FORMAT_TSDB_TYPE_GRAPHITE;
      else if (strcasecmp ("TAGS", value) == 0)
	cb->config.data_format = UTILS_FORMAT_TSDB_TYPE_TAGS;
      else
	{
	  ERROR ("write_tsdb plugin : Invalid data format string: %s", value);
	  return (-1);
	}
    }
  else
    {
      ERROR ("write_tsdb plugin : Invalid key string: %s", ci->key);
      return (-1);
    }

  DEBUG ("write_tsdb plugin : %30s : Ending", "config_set_format");
  return (0);
}				/* }}} int config_set_format */

static int
wt_config_tags (oconfig_item_t * ci, tsdb_config_t * config)	/* {{{ */
{
  DEBUG ("write_tsdb plugin : %30s : Starting", "wt_config_tags");
  int i, t;
  char *name = NULL;

  *config->tags = malloc ((sizeof (tsdb_tag_t)) * 32);
  if (config->tags == NULL)
    {
      ERROR ("write_tsdb plugin : %30s : malloc failed.", "wt_config_tags");
      return (-1);
    }

  if ((ci == NULL) || (config == NULL))
    return (EINVAL);

  t = 0;
  for (i = 0; i < ci->children_num; i++)
    {
      oconfig_item_t *child = ci->children + i;

      DEBUG ("write_tsdb plugin : %30s : Parsing tag : %s",
	     "wt_config_tags", child->key);

      if (child->key)
	{
	  cf_util_get_string (child, &name);
	  DEBUG ("write_tsdb plugin : %30s : Adding tag : %s = %s",
		 "wt_config_tags", child->key, name);

	  config->tags[t] = malloc (sizeof (tsdb_tag_t));
	  if (config->tags[t] == NULL)
	    {
	      ERROR ("write_tsdb plugin : %30s : malloc failed.",
		     "wt_config_tags");
	      return (-1);
	    }

	  snprintf (config->tags[t]->tag, sizeof (config->tags[t]->tag), "%s",
		    child->key);
	  snprintf (config->tags[t]->name, sizeof (config->tags[t]->name),
		    "%s", name);
	  t++;
	}
      else
	{
	  ERROR ("write_tsdb plugin : %30s : Invalid tags", "wt_config_tags");
	  return (EINVAL);
	}
    }
  config->tag_num = t;
  DEBUG ("write_tsdb plugin : %30s : Tags created : %i",
	 "wt_config_tags", config->tag_num);

  sfree (name);

  DEBUG ("write_tsdb plugin : %30s : Ending", "wt_config_tags");
  return (0);
}				/* }}} int wt_config_tags */

static int
wt_config_node (oconfig_item_t * ci)	/* {{{ */
{
  DEBUG ("write_tsdb plugin : %30s : Starting", "wt_config_node");
  wt_callback_t *cb;
  int buffer_size = 0;
  user_data_t user_data;
  char callback_name[DATA_MAX_NAME_LEN];
  int i;

  cb = malloc (sizeof (*cb));
  if (cb == NULL)
    {
      ERROR ("write_tsdb plugin : %30s : malloc failed.", "wt_config_node");
      return (-1);
    }
  memset (cb, 0, sizeof (*cb));

  pthread_mutex_init (&cb->send_lock, /* attr = */ NULL);

  cb->config.verify_peer = 1;
  cb->config.verify_host = 1;
  cb->config.send_format = UTILS_FORMAT_TSDB_SEND_HTTP;
  cb->config.data_format = UTILS_FORMAT_TSDB_TYPE_TAGS;
  cb->config.sslversion = CURL_SSLVERSION_DEFAULT;

  cf_util_get_string (ci, &cb->config.node);

  DEBUG ("write_tsdb plugin : %30s : Node : %s",
	 "wt_config_node", cb->config.node);

  for (i = 0; i < ci->children_num; i++)
    {
      oconfig_item_t *child = ci->children + i;

      DEBUG ("write_tsdb plugin : %30s : Parsing config (%s) : %s",
	     "wt_config_node", cb->config.node, child->key);

      if (strcasecmp ("URL", child->key) == 0)
	cf_util_get_string (child, &cb->config.location);
      else if (strcasecmp ("User", child->key) == 0)
	cf_util_get_string (child, &cb->config.user);
      else if (strcasecmp ("Password", child->key) == 0)
	cf_util_get_string (child, &cb->config.pass);
      else if (strcasecmp ("VerifyPeer", child->key) == 0)
	cf_util_get_boolean (child, &cb->config.verify_peer);
      else if (strcasecmp ("VerifyHost", child->key) == 0)
	cf_util_get_boolean (child, &cb->config.verify_host);
      else if (strcasecmp ("CACert", child->key) == 0)
	cf_util_get_string (child, &cb->config.cacert);
      else if (strcasecmp ("CAPath", child->key) == 0)
	cf_util_get_string (child, &cb->config.capath);
      else if (strcasecmp ("ClientKey", child->key) == 0)
	cf_util_get_string (child, &cb->config.clientkey);
      else if (strcasecmp ("ClientCert", child->key) == 0)
	cf_util_get_string (child, &cb->config.clientcert);
      else if (strcasecmp ("ClientKeyPass", child->key) == 0)
	cf_util_get_string (child, &cb->config.clientkeypass);
      else if (strcasecmp ("SSLVersion", child->key) == 0)
	{
	  char *value = NULL;

	  cf_util_get_string (child, &value);

	  if (value == NULL || strcasecmp ("default", value) == 0)
	    cb->config.sslversion = CURL_SSLVERSION_DEFAULT;
	  else if (strcasecmp ("SSLv2", value) == 0)
	    cb->config.sslversion = CURL_SSLVERSION_SSLv2;
	  else if (strcasecmp ("SSLv3", value) == 0)
	    cb->config.sslversion = CURL_SSLVERSION_SSLv3;
	  else if (strcasecmp ("TLSv1", value) == 0)
	    cb->config.sslversion = CURL_SSLVERSION_TLSv1;
#if (LIBCURL_VERSION_MAJOR > 7) || (LIBCURL_VERSION_MAJOR == 7 && LIBCURL_VERSION_MINOR >= 34)
	  else if (strcasecmp ("TLSv1_0", value) == 0)
	    cb->config.sslversion = CURL_SSLVERSION_TLSv1_0;
	  else if (strcasecmp ("TLSv1_1", value) == 0)
	    cb->config.sslversion = CURL_SSLVERSION_TLSv1_1;
	  else if (strcasecmp ("TLSv1_2", value) == 0)
	    cb->config.sslversion = CURL_SSLVERSION_TLSv1_2;
#endif
	  else
	    ERROR ("write_tsdb plugin : %30s : Invalid SSLVersion "
		   "option: %s.", "wt_config_node", value);

	  sfree (value);
	}
      else if (strcasecmp ("ReplaceChar", child->key) == 0)
	cf_util_get_string (child, &cb->config.replace_char);
      else if (strcasecmp ("DataFormat", child->key) == 0)
	config_set_format (cb, child);
      else if (strcasecmp ("SendFormat", child->key) == 0)
	config_set_format (cb, child);
      else if (strcasecmp ("StoreRates", child->key) == 0)
	cf_util_get_boolean (child, &cb->config.store_rates);
      else if (strcasecmp ("BufferSize", child->key) == 0)
	cf_util_get_int (child, &buffer_size);
      else if (strcasecmp ("Tags", child->key) == 0)
	wt_config_tags (child, &cb->config);
      else if (strcasecmp ("AlwaysAppendDS", child->key) == 0)
	cf_util_get_boolean (child, &cb->config.append_ds);
      else
	{
	  ERROR ("write_tsdb plugin : %30s : Invalid configuration "
		 "option: %s.", "wt_config_node", child->key);
	}
    }

  if (cb->config.location == NULL)
    {
      ERROR ("write_tsdb plugin : %30s : no URL defined for instance '%s'",
	     "wt_config_node", cb->config.node);
      wt_callback_free (cb);
      return (-1);
    }

  /* Determine send_buffer_size. */
  cb->send_buffer_size = WT_DEFAULT_BUFFER_SIZE;
  if (buffer_size >= 1024)
    cb->send_buffer_size = (size_t) buffer_size;
  else if (buffer_size != 0)
    ERROR
      ("write_tsdb plugin : %30s : Ignoring invalid BufferSize setting (%d).",
       "wt_config_node", buffer_size);

  /* Allocate the buffer. */
  cb->send_buffer = malloc (cb->send_buffer_size);
  if (cb->send_buffer == NULL)
    {
      ERROR ("write_tsdb plugin : %30s : malloc(%zu) failed.",
	     "wt_config_node", cb->send_buffer_size);
      wt_callback_free (cb);
      return (-1);
    }
  /* Nulls the buffer and sets ..._free and ..._fill. */
  wt_reset_buffer (cb);

  ssnprintf (callback_name, sizeof (callback_name), "write_tsdb/%s",
	     cb->config.node);
  DEBUG
    ("write_tsdb plugin : %30s : Registering write callback '%s' with URL '%s'",
     "wt_config_node", callback_name, cb->config.location);

  memset (&user_data, 0, sizeof (user_data));
  user_data.data = cb;
  user_data.free_func = NULL;
  plugin_register_flush (callback_name, wt_flush, &user_data);

  user_data.free_func = wt_callback_free;
  plugin_register_write (callback_name, wt_write, &user_data);

  DEBUG ("write_tsdb plugin : %30s : Ending", "wt_config_node");
  return (0);
}				/* }}} int wt_config_node */

static int
wt_config (oconfig_item_t * ci)	/* {{{ */
{
  DEBUG ("write_tsdb plugin : %30s : Starting", "wt_config");
  int i;

  for (i = 0; i < ci->children_num; i++)
    {
      oconfig_item_t *child = ci->children + i;

      if (strcasecmp ("Node", child->key) == 0)
	wt_config_node (child);
      else
	{
	  ERROR ("write_tsdb plugin : %30s : Invalid configuration "
		 "option: %s.", "wt_config", child->key);
	}
    }

  DEBUG ("write_tsdb plugin : %30s : Ending", "wt_config");
  return (0);
}				/* }}} int wt_config */

static int
wt_init (void)			/* {{{ */
{
  DEBUG ("write_tsdb plugin : %30s : Starting", "wt_init");
  /* Call this while collectd is still single-threaded to avoid
   * initialization issues in libgcrypt. */
  curl_global_init (CURL_GLOBAL_SSL);
  DEBUG ("write_tsdb plugin : %30s : Ending", "wt_init");
  return (0);
}				/* }}} int wt_init */

void
module_register (void)		/* {{{ */
{
  DEBUG ("write_tsdb plugin : %30s : Starting", "module_register");
  plugin_register_complex_config ("write_tsdb", wt_config);
  plugin_register_init ("write_tsdb", wt_init);
  DEBUG ("write_tsdb plugin : %30s : Ending", "module_register");
}				/* }}} void module_register */

/* vim: set fdm=marker sw=2 ts=2 tw=78 sts=2 et : */
