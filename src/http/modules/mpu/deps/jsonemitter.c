/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2016, Joyent, Inc.
 */

/*
 * jsonemitter.c: streaming JSON emitter
 */

#include <errno.h>
#include <inttypes.h>
#include <math.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <limits.h>

#include "custr.h"

#include "jsonemitter.h"

/*
 * JSON_MAX_DEPTH is the maximum supported level of nesting of JSON objects.
 */
#define	JSON_MAX_DEPTH	255

typedef enum {
	JSON_NONE,	/* no object is nested at the current depth */
	JSON_OBJECT,	/* an object is nested at the current depth */
	JSON_ARRAY	/* an array is nested at the current depth */
} json_depthdesc_t;

typedef enum {
	JSON_BACKING_STDIO,
	JSON_BACKING_STRING
} json_backing_t;

/*
 * A note on depth management: We allow JSON documents to be nested up to
 * JSON_MAX_DEPTH.  In order to validate output as we emit it, we maintain a
 * stack indicating which type of object is nested at each level of depth
 * (either an array or object).  The stack is recorded in "json_parents", with
 * the top of the stack at "json_parents[json_depth]".  We keep track of the
 * number of object properties or array elements at each level of depth in
 * "json_nemitted[json_depth]".
 */
struct json_emit {
	json_backing_t		json_backing;

	FILE			*json_stream;		/* output stream */
	custr_t			*json_string;		/* output string */

	/* Error conditions. */
	int			json_error_stdio;	/* last stdio error */
	int			json_depth_exceeded;	/* max depth exceeded */
	unsigned int		json_stdio_nskipped;	/* skip due to error */
	unsigned int		json_nbadfloats;	/* bad FP values */

	/* Nesting state. */
	unsigned int		json_depth;
	json_depthdesc_t	json_parents[JSON_MAX_DEPTH + 1];
	unsigned int		json_nemitted[JSON_MAX_DEPTH + 1];

	custr_t			*json_scratch;
	int			json_error_scratch;

	int			json_error_utf8;
	uint8_t			json_error_utf8_byteval;
};

/*
 * General-purpose macros.
 */
#define	VERIFY(x) ((void)((x) || json_assert_fail(#x, __FILE__, __LINE__)))

/*
 * Macros used to apply function attributes.
 */
#define	JSON_PRINTFLIKE2 __attribute__((__format__(__printf__, 2, 3)))

static char json_panicstr[256];
static int json_assert_fail(const char *, const char *, int);

static int json_has_error(json_emit_t *);

static void json_emits(json_emit_t *, const char *);
static void json_emitc(json_emit_t *, char);
static void json_emit_utf8string(json_emit_t *, const char *);
static void json_emit_prepare(json_emit_t *, const char *);

static json_depthdesc_t json_nest_kind(json_emit_t *);
static void json_nest_begin(json_emit_t *, json_depthdesc_t);
static void json_nest_end(json_emit_t *, json_depthdesc_t);


/*
 * Lifecycle management
 */

static json_emit_t *
json_create_common(json_backing_t backing)
{
	json_emit_t *jse;

	if ((jse = calloc(1, sizeof (*jse))) == NULL) {
		return (NULL);
	}

	if (custr_alloc(&jse->json_scratch) != 0) {
		free(jse);
		return (NULL);
	}

	jse->json_backing = backing;

	return (jse);
}

json_emit_t *
json_create_stdio(FILE *outstream)
{
	json_emit_t *jse;

	if ((jse = json_create_common(JSON_BACKING_STDIO)) == NULL) {
		return (NULL);
	}

	jse->json_stream = outstream;

	return (jse);
}

json_emit_t *
json_create_fixed_string(void *buf, size_t buflen)
{
	json_emit_t *jse;

	if ((jse = json_create_common(JSON_BACKING_STRING)) == NULL) {
		return (NULL);
	}

	if (custr_alloc_buf(&jse->json_string, buf, buflen) != 0) {
		int e = errno;

		json_fini(jse);

		errno = e;
		return (NULL);
	}

	return (jse);
}

json_emit_t *
json_create_string(void)
{
	json_emit_t *jse;

	if ((jse = json_create_common(JSON_BACKING_STRING)) == NULL) {
		return (NULL);
	}

	if (custr_alloc(&jse->json_string) != 0) {
		int e = errno;

		json_fini(jse);

		errno = e;
		return (NULL);
	}

	return (jse);
}

void
json_fini(json_emit_t *jse)
{
	custr_free(jse->json_string);
	custr_free(jse->json_scratch);
	free(jse);
}

json_error_t
json_get_error(json_emit_t *jse, char *buf, size_t bufsz)
{
	json_error_t kind;

	if (jse->json_error_stdio != 0) {
		kind = JSE_STDIO;
		(void) snprintf(buf, bufsz, "%s", strerror(errno));
	} else if (jse->json_depth_exceeded != 0) {
		kind = JSE_TOODEEP;
		(void) snprintf(buf, bufsz, "exceeded maximum supported depth");
	} else if (jse->json_nbadfloats) {
		kind = JSE_INVAL;
		(void) snprintf(buf, bufsz, "unsupported floating point value");
	} else if (jse->json_error_utf8 != 0) {
		kind = JSE_INVAL;
		(void) snprintf(buf, bufsz, "illegal UTF-8 byte (0x%02x)",
		    (unsigned)jse->json_error_utf8_byteval);
	} else {
		kind = JSE_NONE;
		if (bufsz > 0) {
			buf[0] = '\0';
		}
	}

	return (kind);
}

/*
 * Get a C string pointer for the current output buffer.  This pointer is
 * invalidated as soon as a mutating function is called.
 */
const char *
json_string_cstr(json_emit_t *jse)
{
	VERIFY(jse->json_backing == JSON_BACKING_STRING);

	/*
	 * The output string can only be read if the state machine has
	 * returned to the rest state, similar to "json_newline()".
	 */
	VERIFY(json_nest_kind(jse) == JSON_NONE);
	VERIFY(jse->json_depth == 0);

	return (custr_cstr(jse->json_string));
}

size_t
json_string_len(json_emit_t *jse)
{
	VERIFY(jse->json_backing == JSON_BACKING_STRING);

	return (custr_len(jse->json_string));
}

/*
 * Clear the accumulated C string.  Can be used after accumulating an
 * entire JSON object for display, or to write to a file or socket,
 * to get a clean slate for the next object.
 */
void
json_string_clear(json_emit_t *jse)
{
	VERIFY(jse->json_backing == JSON_BACKING_STRING);

	/*
	 * The output string can only be reset if the state machine has
	 * returned to the rest state, similar to "json_newline()".
	 */
	VERIFY(json_nest_kind(jse) == JSON_NONE);
	VERIFY(jse->json_depth == 0);

	custr_reset(jse->json_string);
}


/*
 * Helper functions
 */

static int
json_assert_fail(const char *cond, const char *file, int line)
{
	int nbytes;
	nbytes = snprintf(json_panicstr, sizeof (json_panicstr),
	    "libjsonemitter assertion failed: %s at file %s line %d\n",
	    cond, file, line);
	(void) write(STDERR_FILENO, json_panicstr, nbytes);
	abort();
}

/*
 * Returns true if we've seen any error up to this point.
 */
static int
json_has_error(json_emit_t *jse)
{
	return (jse->json_error_stdio != 0 || jse->json_depth_exceeded != 0 ||
	    jse->json_error_scratch != 0 || jse->json_error_utf8 != 0);
}

static void
json_emitc(json_emit_t *jse, char c)
{
	if (json_has_error(jse)) {
		jse->json_stdio_nskipped++;
		return;
	}

	switch (jse->json_backing) {
	case JSON_BACKING_STDIO:
		if (fprintf(jse->json_stream, "%c", c) < 0) {
			jse->json_error_stdio = errno;
		}
		break;

	case JSON_BACKING_STRING:
		if (custr_appendc(jse->json_string, c) != 0) {
			jse->json_error_stdio = errno;
		}
		break;
	}
}

static void
json_emits(json_emit_t *jse, const char *s)
{
	if (json_has_error(jse)) {
		jse->json_stdio_nskipped++;
		return;
	}

	switch (jse->json_backing) {
	case JSON_BACKING_STDIO:
		if (fprintf(jse->json_stream, "%s", s) < 0) {
			jse->json_error_stdio = errno;
		}
		break;

	case JSON_BACKING_STRING:
		if (custr_append(jse->json_string, s) != 0) {
			jse->json_error_stdio = errno;
		}
		break;
	}
}

static void
json_scratch_appendc(json_emit_t *jse, char c)
{
	if (json_has_error(jse)) {
		return;
	}

	if (custr_appendc(jse->json_scratch, c) != 0) {
		jse->json_error_scratch = errno;
		return;
	}
}

/*
 * Emits a UTF-8 (or 7-bit clean ASCII) string, with appropriate translation of
 * characters that must be escaped in the JSON representation.
 */
static void
json_emit_utf8string(json_emit_t *jse, const char *utf8str)
{
	unsigned utf8_more_bytes = 0;

	custr_reset(jse->json_scratch);

	for (const char *cp = utf8str; *cp != '\0'; cp++) {
		char c = *cp;
		unsigned char code = c;

		if (utf8_more_bytes > 0) {
			/*
			 * We need to collect one or more additional
			 * bytes to complete this UTF-8 multibyte character.
			 *
			 * Every continuation byte matches the bit pattern:
			 * 	10xxxxxx
			 */
			if ((code & 0xC0) != 0x80) {
				/*
				 * This is not a valid continuation byte.
				 */
				jse->json_error_utf8 = EILSEQ;
				jse->json_error_utf8_byteval = code;
				return;
			}

			json_scratch_appendc(jse, c);
			utf8_more_bytes--;
			continue;
		}

		switch (c) {
		/*
		 * Regular characters that must be escaped include the
		 * string delimiter itself (quotation mark), and the
		 * escape sequence initiator (reverse solidus):
		 */
		case '"':
			json_scratch_appendc(jse, '\\');
			json_scratch_appendc(jse, '\"');
			continue;
		case '\\':
			json_scratch_appendc(jse, '\\');
			json_scratch_appendc(jse, '\\');
			continue;

		/*
		 * Control characters with C-style escape sequences:
		 */
		case '\b':
			json_scratch_appendc(jse, '\\');
			json_scratch_appendc(jse, 'b');
			continue;
		case '\f':
			json_scratch_appendc(jse, '\\');
			json_scratch_appendc(jse, 'f');
			continue;
		case '\n':
			json_scratch_appendc(jse, '\\');
			json_scratch_appendc(jse, 'n');
			continue;
		case '\r':
			json_scratch_appendc(jse, '\\');
			json_scratch_appendc(jse, 'r');
			continue;
		case '\t':
			json_scratch_appendc(jse, '\\');
			json_scratch_appendc(jse, 't');
			continue;
		}

		if (code <= 0x1F) {
			/*
			 * This is a control character that was not handled
			 * above.  Use the four hex digit escape sequence.
			 */
			char num[5];

			(void) snprintf(num, 5, "%04x", (unsigned)code);

			json_scratch_appendc(jse, '\\');
			json_scratch_appendc(jse, 'u');
			json_scratch_appendc(jse, num[0]);
			json_scratch_appendc(jse, num[1]);
			json_scratch_appendc(jse, num[2]);
			json_scratch_appendc(jse, num[3]);
			continue;
		}

		if (code <= 0x7F) {
			/*
			 * This is a regular ASCII character, and may be copied
			 * directly into the output string.
			 */
			json_scratch_appendc(jse, c);
			continue;
		}

		/*
		 * Check for a UTF-8 multibyte character.
		 *
		 * 	2-byte		110xxxxx
		 * 	3-byte		1110xxxx
		 * 	4-byte		11110xxx
		 */
		if ((code & 0xE0) == 0xC0) {
			utf8_more_bytes = 1;
		} else if ((code & 0xF0) == 0xE0) {
			utf8_more_bytes = 2;
		} else if ((code & 0xF8) == 0xF0) {
			utf8_more_bytes = 3;
		} else {
			/*
			 * This is not a valid UTF-8 character.
			 */
			jse->json_error_utf8 = EILSEQ;
			jse->json_error_utf8_byteval = code;
			return;
		}

		/*
		 * For a valid UTF-8 multibyte character, we must copy all
		 * related bytes into the output string together.  Copy the
		 * first now; subsequent bytes will be checked and copied
		 * because we set "utf8_more_bytes" above.
		 */
		json_scratch_appendc(jse, c);
	}

	if (utf8_more_bytes != 0) {
		/*
		 * The string ended in the middle of a character.
		 */
		jse->json_error_utf8 = EILSEQ;
		return;
	}

	json_emitc(jse, '"');
	json_emits(jse, custr_cstr(jse->json_scratch));
	json_emitc(jse, '"');
}

/*
 * Each function that emits any kind of object (including the functions that
 * begin emitting objects and arrays) takes a "label" parameter.  This must be
 * non-null if and only if we're inside an object.  For convenience, every
 * function always calls this helper with the label that's provided.  We verify
 * the argument is consistent with our state, and then we emit the label if we
 * need to.
 *
 * It is illegal to call this function if we've experienced any stdio errors
 * already.
 */
static void
json_emit_prepare(json_emit_t *jse, const char *label)
{
	json_depthdesc_t kind;

	kind = json_nest_kind(jse);
	if ((kind == JSON_OBJECT || kind == JSON_ARRAY) &&
	    jse->json_nemitted[jse->json_depth] > 0) {
		json_emitc(jse, ',');
	}

	if (label == NULL) {
		VERIFY(kind != JSON_OBJECT);
		return;
	}

	VERIFY(kind == JSON_OBJECT);
	json_emit_utf8string(jse, label);
	json_emitc(jse, ':');
}

static void
json_emit_finish(json_emit_t *jse)
{
	jse->json_nemitted[jse->json_depth]++;
}

static json_depthdesc_t
json_nest_kind(json_emit_t *jse)
{
	json_depthdesc_t kind;
	VERIFY(jse->json_depth <= JSON_MAX_DEPTH);
	kind = jse->json_parents[jse->json_depth];
	VERIFY(jse->json_depth == 0 ||
	    kind == JSON_OBJECT || kind == JSON_ARRAY);
	return (kind);
}

static void
json_nest_begin(json_emit_t *jse, json_depthdesc_t kind)
{
	VERIFY(kind == JSON_OBJECT || kind == JSON_ARRAY);

	if (json_has_error(jse)) {
		return;
	}

	if (jse->json_depth == JSON_MAX_DEPTH) {
		jse->json_depth_exceeded = 1;
		return;
	}

	jse->json_depth++;
	jse->json_parents[jse->json_depth] = kind;
	jse->json_nemitted[jse->json_depth] = 0;
}

static void
json_nest_end(json_emit_t *jse, json_depthdesc_t kind)
{
	VERIFY(kind == JSON_OBJECT || kind == JSON_ARRAY);
	if (json_has_error(jse)) {
		return;
	}

	VERIFY(json_nest_kind(jse) == kind);
	VERIFY(jse->json_depth > 0);
	jse->json_depth--;
}

static void
json_emit_common(json_emit_t *jse, const char *label, const char *barevalue)
{
	json_emit_prepare(jse, label);
	json_emits(jse, barevalue);
	json_emit_finish(jse);
}

/*
 * Public emitter functions.
 */

void
json_object_begin(json_emit_t *jse, const char *label)
{
	json_emit_prepare(jse, label);
	json_emitc(jse, '{');
	json_nest_begin(jse, JSON_OBJECT);
}

void
json_object_end(json_emit_t *jse)
{
	json_nest_end(jse, JSON_OBJECT);
	json_emitc(jse, '}');
	json_emit_finish(jse);
}

void
json_array_begin(json_emit_t *jse, const char *label)
{
	json_emit_prepare(jse, label);
	json_emitc(jse, '[');
	json_nest_begin(jse, JSON_ARRAY);
}

void
json_array_end(json_emit_t *jse)
{
	json_nest_end(jse, JSON_ARRAY);
	json_emitc(jse, ']');
	json_emit_finish(jse);
}

void
json_newline(json_emit_t *jse)
{
	if (json_has_error(jse)) {
		return;
	}

	VERIFY(json_nest_kind(jse) == JSON_NONE);
	VERIFY(jse->json_depth == 0);
	json_emitc(jse, '\n');
}

void
json_boolean(json_emit_t *jse, const char *label, json_boolean_t value)
{
	VERIFY(value == JSON_B_FALSE || value == JSON_B_TRUE);

	json_emit_common(jse, label, value == JSON_B_TRUE ? "true" : "false");
}

void
json_null(json_emit_t *jse, const char *label)
{
	json_emit_common(jse, label, "null");
}

void
json_int64(json_emit_t *jse, const char *label, int64_t value)
{
	char buf[96];

	(void) snprintf(buf, sizeof (buf), "%" PRId64, value);

	json_emit_common(jse, label, buf);
}

void
json_uint64(json_emit_t *jse, const char *label, uint64_t value)
{
	char buf[96];

	(void) snprintf(buf, sizeof (buf), "%" PRIu64, value);

	json_emit_common(jse, label, buf);
}

void
json_double(json_emit_t *jse, const char *label, double value)
{
	char buf[96];

	if (!isfinite(value)) {
		jse->json_nbadfloats++;
		return;
	}

	(void) snprintf(buf, sizeof (buf), "%.10e", value);

	json_emit_common(jse, label, buf);
}

void
json_utf8string(json_emit_t *jse, const char *label, const char *value)
{
	json_emit_prepare(jse, label);
	json_emit_utf8string(jse, value);
	json_emit_finish(jse);
}
