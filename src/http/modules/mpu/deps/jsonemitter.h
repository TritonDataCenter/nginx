/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2016, Joyent, Inc.
 */

/*
 * jsonemitter.h: interface for emitting streaming JSON
 *
 * The interfaces here enable callers to emit properly formatted JSON (as
 * defined by ECMA-404) in a streaming way.
 *
 * (1) Initialization and cleanup
 *
 *     You instantiate an emitter using:
 *
 *         FILE *stream = ...
 *         json_emit_t *jse = json_create_stdio(stream);
 *
 *     "stream" is a stdio stream to which the JSON output will be emitted.
 *
 *     json_create_stdio() returns NULL on failure with errno set appropriately.
 *
 *     When you've completed the operation and checked for errors, use
 *     json_fini() to free resources created by the emitter.  After that, no
 *     other functions may be called using the emitter.  (This does nothing to
 *     the underlying stdio stream.  The caller may wish to flush or close that
 *     stream.)
 *
 * (2) Emitting data
 *
 *     You can emit primitive types of data using a combination of the
 *     functions:
 *
 *        json_boolean()
 *        json_null()
 *        json_int64()
 *        json_uint64()
 *        json_double()*
 *        json_utf8string()
 *
 *     Note that double-precision floating-point values are emitted with ten
 *     digits in the current implementation, but this is subject to change to
 *     in the future without sacrificing precision.
 *
 *     You can emit objects and arrays using the functions:
 *
 *        json_object_begin(), json_object_end()
 *        json_array_begin(), json_array_end()
 *
 *     All of the emitter functions return void.  See "Error handling".
 *
 *     All of the emitter functions take an optional "label" argument that may
 *     be NULL or a UTF-8 string.  The label must be specified if and only if
 *     this call is between matching calls to json_object_begin() and
 *     json_object_end().  The label is used as the name of the property to be
 *     emitted.  For example:
 *
 *         json_object_begin(jse, NULL);
 *         json_int64(jse, "nerrors", 37);
 *         json_object_end(jse);
 *
 *     would emit the string:
 *
 *         {"nerrors":37}
 *
 * (3) Newlines
 *
 *     A single emitter can be used to emit multiple top-level JSON values in
 *     sequence.  This is primarily intended for emitting documents consisting
 *     of newline-separated JSON.  You can use json_newline() to emit a newline
 *     betwen values.  This function can only be used at the top level (i.e.,
 *     not inside objects or arrays).
 *
 * (4) Error handling
 *
 *     There are several operational errors that can happen while emitting JSON.
 *     These are currently:
 *
 *         JSE_STDIO	An error was encountered calling a stdio function.
 *
 *         JSE_TOODEEP	The caller attempted to emit more than the supported
 *         		number of nested objects or arrays.  Currently, 255 is
 *         		the maximum level of nesting that's supported.
 *
 *         JSE_INVAL	The caller attempted to emit an unsupported value.  This
 *         		currently can only happen if the caller attempts to emit
 *         		a floating-point value that's infinite or NaN.
 *
 *     It is a programmer error to improperly nest objects and arrays, to
 *     provide labels for values that are not inside objects, or to provide no
 *     labels for values that are inside objects.
 *
 *     The programming interface is optimized for use cases where the caller
 *     will either attempt to emit an entire JSON document and then check
 *     whether that completed successfully, or they will emit the document in
 *     pieces and abort a higher-level operation if emitting the JSON document
 *     fails.  The emitter functions return void.  To check if there's been any
 *     error up to this point, use:
 *
 *         json_error_t error = json_get_error(jse, buf, bufsz);
 *
 *     "error" will be either JSE_NONE or one of the values above.  "buf" and
 *     "bufsz" work like snprintf(3C): if "bufsz" is positive, then a
 *     descriptive error message will be copied into the buffer "buf", which
 *     should be at least "bufsz" bytes.  If "bufsz" is 0, no message is copied,
 *     and the value of "buf" is ignored.
 */

#ifndef	_JSONEMITTER_H
#define	_JSONEMITTER_H

#include <stdint.h>
#include <stdio.h>

typedef struct json_emit json_emit_t;

typedef enum {
	JSON_B_FALSE,
	JSON_B_TRUE
} json_boolean_t;

typedef enum {
	JSE_NONE,	/* no error */
	JSE_STDIO,	/* error from stdio function */
	JSE_TOODEEP,	/* exceeded maximum supported depth */
	JSE_INVAL,	/* unsupported value emitted (e.g., NaN) */
} json_error_t;

json_emit_t *json_create_stdio(FILE *);
json_emit_t *json_create_string(void);
json_emit_t *json_create_fixed_string(void *, size_t);
json_error_t json_get_error(json_emit_t *, char *, size_t);
void json_fini(json_emit_t *);

void json_object_begin(json_emit_t *, const char *);
void json_object_end(json_emit_t *);

void json_array_begin(json_emit_t *, const char *);
void json_array_end(json_emit_t *);

void json_newline(json_emit_t *);

void json_boolean(json_emit_t *, const char *, json_boolean_t);
void json_null(json_emit_t *, const char *);
void json_int64(json_emit_t *, const char *, int64_t);
void json_uint64(json_emit_t *, const char *, uint64_t);
void json_double(json_emit_t *, const char *, double);
void json_utf8string(json_emit_t *, const char *, const char *);

/*
 * For use with emitters created via json_create_string():
 */
const char *json_string_cstr(json_emit_t *);
size_t json_string_len(json_emit_t *);
void json_string_clear(json_emit_t *);

#endif /* not defined _JSONEMITTER_H_ */
