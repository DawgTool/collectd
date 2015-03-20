/**
 * collectd - src/utils_format_tsdb.c
 * Copyright (C) 2015       Dallin Young
 * Copyright (C) 2009       Florian octo Forster
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
 *   Dallin Young <dallin.young@gmail.com>
 *   Florian octo Forster <octo at collectd.org>
 **/

#include "collectd.h"
#include "plugin.h"
#include "common.h"

#include "utils_cache.h"
#include "utils_format_tsdb.h"

#define CHAR_FORBIDDEN " \t\"\\:!/()\n\r"

static char *
remove_forbidden_chars (const char *buffer, const char *replace_char)
{
  DEBUG ("format_tsdb utils : %30s : Starting", "remove_forbidden_chars");
  char *rchar = NULL;
  char *rbuff = NULL;
  int i;

  rchar = strdup (replace_char);
  rbuff = strdup (buffer);

  if (strchr (CHAR_FORBIDDEN, replace_char[0]) != NULL ||
      strlen (replace_char) != 1)
    {
      WARNING ("format_tsdb utils : %30s : replace character invalid"
	       " : %s", "remove_forbidden_chars", replace_char);
      rchar = strdup ("_");
    }

  for (i = 0; i < strlen (rbuff); i++)
    {
      if strchr
	(CHAR_FORBIDDEN, buffer[i])
	{
	  DEBUG ("format_tsdb utils : %30s : buffer replace : %c",
		 "remove_forbidden_chars", rbuff[i]);
	  rbuff[i] = rchar[0];
	}
      else
	{
	  DEBUG ("format_tsdb utils : %30s : buffer character : %c",
		 "remove_forbidden_chars", rbuff[i]);
	}
    }
  sfree (rchar);

  DEBUG ("format_tsdb utils : %30s : Ending", "remove_forbidden_chars");
  return (rbuff);
}				/* }}} void remove_forbidden_chars */

static int
value_to_record (tsdb_rec_t * tsdb,	/* {{{ */
		 const data_set_t * ds, const value_list_t * vl,
		 const tsdb_config_t * config)
{
  DEBUG ("format_tsdb utils : %30s : Starting", "value_to_record");

  char *prefix = NULL;
  const char *meta_prefix = "tsdb_prefix";
  int status, i, t;
  status = 0;

  DEBUG ("format_tsdb utils : %30s : ds type = %s : ds num = %d : "
	 "vl type = %s", "value_to_record", ds->type, ds->ds_num, vl->type);
  if (0 != strcmp (ds->type, vl->type))
    {
      ERROR ("format_tsdb utils : %30s : DS type does not match"
	     "value list type", "value_to_record");
      return (-1);
    }

  if (vl->meta)
    {
      status = meta_data_get_string (vl->meta, meta_prefix, &prefix);
      if (status == -ENOENT)
	{
	  prefix = NULL;
	}
      else if (status < 0)
	{
	  return (-1);
	}
    }


  for (i = 0; i < ds->ds_num; i++)
    {
      t = 0;

      tsdb[i].timestamp = CDTIME_T_TO_MS (vl->time);

      snprintf (tsdb[i].tags[t].tag, sizeof (tsdb[i].tags[t].tag), "%s",
		"host");
      snprintf (tsdb[i].tags[t].name, sizeof (tsdb[i].tags[t].name), "%s",
		vl->host);
      t++;

      if (ds->ds[i].type == DS_TYPE_COUNTER)
	snprintf (tsdb[i].value, sizeof (tsdb[i].value), "%llu",
		  vl->values[i].counter);
      else if (ds->ds[i].type == DS_TYPE_DERIVE)
	snprintf (tsdb[i].value, sizeof (tsdb[i].value), "%" PRIi64,
		  vl->values[i].derive);
      else if (ds->ds[i].type == DS_TYPE_ABSOLUTE)
	snprintf (tsdb[i].value, sizeof (tsdb[i].value), "%" PRIu64,
		  vl->values[i].absolute);
      else if (ds->ds[i].type == DS_TYPE_GAUGE)
	if (isfinite (vl->values[i].gauge))
	  snprintf (tsdb[i].value, sizeof (tsdb[i].value), "%g",
		    vl->values[i].gauge);
	else if (config->store_rates)
	  snprintf (tsdb[i].value, sizeof (tsdb[i].value), "%g",
		    (uc_get_rate (ds, vl)[i]));
	else
	  {
	    ERROR ("format_tsdb utils : %30s : Unknown gauge value",
		   "value_to_record");
	    return (-1);
	  }
      else
	{
	  ERROR ("format_tsdb utils : %30s : Unknown data source type : %i",
		 "value_to_record", ds->ds[i].type);
	  return (-1);
	}

      if (config->data_format == UTILS_FORMAT_TSDB_TYPE_GRAPHITE)
	{
	  snprintf (tsdb[i].metric, sizeof (tsdb[i].metric),
		    "%s%s%s%s%s%s%s%s%s%s%s%s%s", (prefix) ? prefix : "",
		    (prefix) ? "." : "",
		    (vl->plugin[0] != '\0') ? vl->plugin : "",
		    (vl->plugin_instance[0] != '\0') ? "." : "",
		    (vl->plugin_instance[0] !=
		     '\0') ? vl->plugin_instance : "",
		    (vl->type[0] != '\0') ? "." : "",
		    (vl->type[0] != '\0') ? vl->type : "",
		    (vl->type_instance[0] != '\0') ? "." : "",
		    (vl->type_instance[0] != '\0') ? vl->type_instance : "",
		    (ds->ds[i].name[0] != '\0'
		     && 0 != strcmp (ds->ds[i].name, "value")) ? "." : "",
		    (ds->ds[i].name[0] != '\0'
		     && 0 != strcmp (ds->ds[i].name,
				     "value")) ? ds->ds[i].name : "",
		    (config->append_ds
		     && ds->ds[i].type) ? "." : "", (config->append_ds
						     && ds->ds[i].type) ?
		    DS_TYPE_TO_STRING (ds->ds[i].type) : "");
	  DEBUG ("format_tsdb utils : %30s : Setting : %s = %s",
		 "value_to_record", "Metric", tsdb[i].metric);
	}
      else
	{
	  snprintf (tsdb[i].metric, sizeof (tsdb[i].metric), "%s",
		    (vl->plugin[0] != '\0') ? vl->plugin : vl->type);
	  DEBUG ("format_tsdb utils : %30s : Setting : %s = %s",
		 "value_to_record", "Metric", tsdb[i].metric);

	  snprintf (tsdb[i].tags[t].tag, sizeof (tsdb[i].tags[t].tag), "%s",
		    "interval");
	  snprintf (tsdb[i].tags[t].name, sizeof (tsdb[i].tags[t].name),
		    "%ld", CDTIME_T_TO_MS (vl->interval));
	  DEBUG ("format_tsdb utils : %30s : Setting : %s = %s",
		 "value_to_record", tsdb[i].tags[t].tag,
		 tsdb[i].tags[t].name);
	  t++;

	  if (prefix)
	    {
	      snprintf (tsdb[i].tags[t].tag, sizeof (tsdb[i].tags[t].tag),
			"%s", "prefix");
	      snprintf (tsdb[i].tags[t].name, sizeof (tsdb[i].tags[t].name),
			"%s", prefix);
	      DEBUG ("format_tsdb utils : %30s : Setting : %s = %s",
		     "value_to_record", tsdb[i].tags[t].tag,
		     tsdb[i].tags[t].name);
	      t++;
	    }
	  if (vl->plugin[0] != '\0')
	    {
	      snprintf (tsdb[i].tags[t].tag, sizeof (tsdb[i].tags[t].tag),
			"%s", "plugin");
	      snprintf (tsdb[i].tags[t].name, sizeof (tsdb[i].tags[t].name),
			"%s", vl->plugin);
	      DEBUG ("format_tsdb utils : %30s : Setting : %s = %s",
		     "value_to_record", tsdb[i].tags[t].tag,
		     tsdb[i].tags[t].name);
	      t++;
	    }
	  if (vl->plugin_instance[0] != '\0')
	    {
	      snprintf (tsdb[i].tags[t].tag, sizeof (tsdb[i].tags[t].tag),
			"%s", "plugin_instance");
	      snprintf (tsdb[i].tags[t].name, sizeof (tsdb[i].tags[t].name),
			"%s", vl->plugin_instance);
	      DEBUG ("format_tsdb utils : %30s : Setting : %s = %s",
		     "value_to_record", tsdb[i].tags[t].tag,
		     tsdb[i].tags[t].name);
	      t++;
	    }
	  if (vl->type[0] != '\0')
	    {
	      snprintf (tsdb[i].tags[t].tag, sizeof (tsdb[i].tags[t].tag),
			"%s", "type");
	      snprintf (tsdb[i].tags[t].name, sizeof (tsdb[i].tags[t].name),
			"%s", vl->type);
	      DEBUG ("format_tsdb utils : %30s : Setting : %s = %s",
		     "value_to_record", tsdb[i].tags[t].tag,
		     tsdb[i].tags[t].name);
	      t++;
	    }
	  if (vl->type_instance[0] != '\0')
	    {
	      snprintf (tsdb[i].tags[t].tag, sizeof (tsdb[i].tags[t].tag),
			"%s", "type_instance");
	      snprintf (tsdb[i].tags[t].name, sizeof (tsdb[i].tags[t].name),
			"%s", vl->type_instance);
	      DEBUG ("format_tsdb utils : %30s : Setting : %s = %s",
		     "value_to_record", tsdb[i].tags[t].tag,
		     tsdb[i].tags[t].name);
	      t++;
	    }
	  if (ds->ds[i].name[0] != '\0'
	      && 0 != strcmp (ds->ds[i].name, "value"))
	    {
	      snprintf (tsdb[i].tags[t].tag, sizeof (tsdb[i].tags[t].tag),
			"%s", "data_name");
	      snprintf (tsdb[i].tags[t].name, sizeof (tsdb[i].tags[t].name),
			"%s", ds->ds[i].name);
	      DEBUG ("format_tsdb utils : %30s : Setting : %s = %s",
		     "value_to_record", tsdb[i].tags[t].tag,
		     tsdb[i].tags[t].name);
	      t++;
	    }
	  if (config->append_ds && ds->ds[i].type)
	    {
	      snprintf (tsdb[i].tags[t].tag, sizeof (tsdb[i].tags[t].tag),
			"%s", "data_type");
	      snprintf (tsdb[i].tags[t].name, sizeof (tsdb[i].tags[t].name),
			"%s", DS_TYPE_TO_STRING (ds->ds[i].type));
	      DEBUG ("format_tsdb utils : %30s : Setting : %s = %s",
		     "value_to_record", tsdb[i].tags[t].tag,
		     tsdb[i].tags[t].name);
	      t++;
	    }
	}
      tsdb[i].tag_num = t;
    }

  sfree (prefix);

  DEBUG ("format_tsdb utils : %30s : Ending", "value_to_record");
  return (0);
}				/* }}} int value_to_record */

static int
value_list_to_json (char *buffer, size_t buffer_size,	/* {{{ */
		    const data_set_t * ds, const value_list_t * vl,
		    const tsdb_config_t * config)
{
  DEBUG ("format_tsdb utils : %30s : Starting", "value_list_to_json");

/*
EXAMPLE (GRAPHITE):
{
      "metric": "cpu.1.cpu.nice.value",
      "timestamp": 1346846400,
      "value": 18,
      "tags": {
         "host": "web01",
         "dc": "lga"
      }
}
EXAMPLE (TAGS):
{
      "metric": "cpu",
      "timestamp": 1346846400,
      "value": 18,
      "tags": {
         "host": "web01",
         "dc": "lga",
         "plugin": "cpu",
         "plugin_instance": "1",
         "type": "cpu",
         "type_instance": "nice",
         "dstypes": "derive",
         "dsnames": "value"
      }
}
*/

  size_t offset = 0;
  int status, i, t;

  tsdb_rec_t tsdb[32];
  memset (tsdb, 0, (sizeof (tsdb_rec_t)) * 32);

  status = value_to_record (tsdb, ds, vl, config);
  if (status != 0)
    {
      ERROR ("format_tsdb utils : %30s : failed to get records.",
	     "value_list_to_json");
      return (status);
    }
#define BUFFER_ADD(...) do { \
    status = ssnprintf (buffer + offset, buffer_size - offset, \
        __VA_ARGS__); \
    if (status < 1) \
      return (-1); \
    else if (((size_t) status) >= (buffer_size - offset)) \
      return (-ENOMEM); \
    else \
      offset += ((size_t) status); \
} while (0)


  for (i = 0; i < ds->ds_num; i++)
    {
      /* All value lists have a leading comma. The first one will be replaced with
       * a square bracket in `format_tsdb_finalize'. */
      BUFFER_ADD (",{");

      DEBUG ("format_tsdb utils : %30s : \"metric\": \"%s\",",
	     "value_list_to_json",
	     remove_forbidden_chars (tsdb[i].metric, config->replace_char));
      BUFFER_ADD ("\"metric\":\"%s\",",
		  remove_forbidden_chars (tsdb[i].metric,
					  config->replace_char));

      DEBUG ("format_tsdb utils : %30s : \"timestamp\": %ld,",
	     "value_list_to_json", tsdb[i].timestamp);
      BUFFER_ADD ("\"timestamp\":%ld,", tsdb[i].timestamp);

      DEBUG ("format_tsdb utils : %30s : \"value\": %s,",
	     "value_list_to_json", tsdb[i].value);
      BUFFER_ADD ("\"value\":%s,", tsdb[i].value);

      DEBUG ("format_tsdb utils : %30s : \"tags(%i)\": {",
	     "value_list_to_json", (tsdb[i].tag_num + config->tag_num));
      BUFFER_ADD ("\"tags\":{");

      for (t = 0; t < tsdb[i].tag_num; t++)
	{
	  DEBUG ("format_tsdb utils : %30s : \"%s\": \"%s\",",
		 "value_list_to_json",
		 remove_forbidden_chars (tsdb[i].tags[t].tag,
					 config->replace_char),
		 remove_forbidden_chars (tsdb[i].tags[t].name,
					 config->replace_char));
	  BUFFER_ADD ("\"%s\":\"%s\"%s",
		      remove_forbidden_chars (tsdb[i].tags[t].tag,
					      config->replace_char),
		      remove_forbidden_chars (tsdb[i].tags[t].name,
					      config->replace_char),
		      (t < (tsdb[i].tag_num - 1) ? "," : ""));
	}

      BUFFER_ADD ("%s", (config->tag_num > 0 ? "," : ""));

      for (t = 0; t < config->tag_num; t++)
	{
	  DEBUG ("format_tsdb utils : %30s : \"%s\": \"%s\",",
		 "value_list_to_json",
		 remove_forbidden_chars (config->tags[t]->tag,
					 config->replace_char),
		 remove_forbidden_chars (config->tags[t]->name,
					 config->replace_char));
	  BUFFER_ADD ("\"%s\":\"%s\"%s",
		      remove_forbidden_chars (config->tags[t]->tag,
					      config->replace_char),
		      remove_forbidden_chars (config->tags[t]->name,
					      config->replace_char),
		      (t < (config->tag_num - 1) ? "," : ""));
	}

      DEBUG ("format_tsdb utils : %30s : }", "value_list_to_json");
      BUFFER_ADD ("}");

      BUFFER_ADD ("}");
    }

#undef BUFFER_ADD

  INFO ("format_tsdb utils : %30s : buffer = %s;", "value_list_to_json",
	buffer);

  DEBUG ("format_tsdb utils : %30s : Ending", "value_list_to_json");
  return (0);
}				/* }}} int value_list_to_json */

static int
format_tsdb_value_list_nocheck (char *buffer,	/* {{{ */
				size_t * ret_buffer_fill,
				size_t * ret_buffer_free,
				const data_set_t * ds,
				const value_list_t * vl, size_t temp_size,
				const tsdb_config_t * config)
{
  DEBUG ("format_tsdb utils : %30s : Starting",
	 "format_tsdb_value_list_nocheck");
  char temp[temp_size];
  int status;

  if (config->send_format == UTILS_FORMAT_TSDB_SEND_HTTP)
    {
      /* value_list_to_http */
      status = value_list_to_json (temp, sizeof (temp), ds, vl, config);
    }
  else if (config->send_format == UTILS_FORMAT_TSDB_SEND_PUT)
    {
      /* value_list_to_put */
      status = value_list_to_json (temp, sizeof (temp), ds, vl, config);
    }
  else
    {
      ERROR ("format_tsdb : %30s : Unknown Send Format",
	     "format_tsdb_value_list_nocheck");
      status = -1;
    }

  if (status != 0)
    return (status);
  temp_size = strlen (temp);

  memcpy (buffer + (*ret_buffer_fill), temp, temp_size + 1);
  (*ret_buffer_fill) += temp_size;
  (*ret_buffer_free) -= temp_size;

  INFO ("format_tsdb utils : %30s : (Size) : Temp = %zu : "
	"Buffer Fill = %zu : "
	"Buffer Free = %zu",
	"format_tsdb_value_list_nocheck",
	temp_size, (*ret_buffer_fill), (*ret_buffer_free));

  DEBUG ("format_tsdb utils : %30s : Ending",
	 "format_tsdb_value_list_nocheck");
  return (0);
}				/* }}} int format_tsdb_value_list_nocheck */

int
format_tsdb_initialize (char *buffer,	/* {{{ */
			size_t * ret_buffer_fill, size_t * ret_buffer_free)
{
  DEBUG ("format_tsdb utils : %30s : Starting", "format_tsdb_initialize");
  size_t buffer_fill;
  size_t buffer_free;

  if ((buffer == NULL) || (ret_buffer_fill == NULL)
      || (ret_buffer_free == NULL))
    return (-EINVAL);

  buffer_fill = *ret_buffer_fill;
  buffer_free = *ret_buffer_free;

  buffer_free = buffer_fill + buffer_free;
  buffer_fill = 0;

  if (buffer_free < 3)
    return (-ENOMEM);

  memset (buffer, 0, buffer_free);
  *ret_buffer_fill = buffer_fill;
  *ret_buffer_free = buffer_free;

  DEBUG ("format_tsdb utils : %30s : Ending", "format_tsdb_initialize");
  return (0);
}				/* }}} int format_tsdb_initialize */

int
format_tsdb_finalize (char *buffer,	/* {{{ */
		      size_t * ret_buffer_fill, size_t * ret_buffer_free)
{
  DEBUG ("format_tsdb utils : %30s : Starting", "format_tsdb_finalize");
  size_t pos;

  if ((buffer == NULL) || (ret_buffer_fill == NULL)
      || (ret_buffer_free == NULL))
    return (-EINVAL);

  if (*ret_buffer_free < 2)
    return (-ENOMEM);

  /* Replace the leading comma added in `value_list_to_json' with a square
   * bracket. */
  if (buffer[0] != ',')
    return (-EINVAL);
  buffer[0] = '[';

  pos = *ret_buffer_fill;
  buffer[pos] = ']';
  buffer[pos + 1] = 0;

  (*ret_buffer_fill)++;
  (*ret_buffer_free)--;

  DEBUG ("format_tsdb utils : %30s : Ending", "format_tsdb_finalize");
  return (0);
}				/* }}} int format_tsdb_finalize */

int
format_tsdb_value_list (char *buffer,	/* {{{ */
			size_t * ret_buffer_fill, size_t * ret_buffer_free,
			const data_set_t * ds, const value_list_t * vl,
			const tsdb_config_t * config)
{
  DEBUG ("format_tsdb utils : %30s : Starting", "format_tsdb_value_list");
  if ((buffer == NULL)
      || (ret_buffer_fill == NULL) || (ret_buffer_free == NULL)
      || (ds == NULL) || (vl == NULL))
    return (-EINVAL);

  if (*ret_buffer_free < 3)
    return (-ENOMEM);

  return (format_tsdb_value_list_nocheck (buffer,
					  ret_buffer_fill, ret_buffer_free,
					  ds, vl, ((*ret_buffer_free) - 2),
					  config));
  DEBUG ("format_tsdb utils : %30s : Ending", "format_tsdb_value_list");
}				/* }}} int format_tsdb_value_list */

/* vim: set fdm=marker sw=2 ts=2 tw=78 sts=2 et : */
