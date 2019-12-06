/*
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 * 1. Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY THE AUTHOR AND CONTRIBUTORS ``AS IS'' AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED.  IN NO EVENT SHALL THE AUTHOR OR CONTRIBUTORS BE LIABLE
 * FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS
 * OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
 * HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
 * LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY
 * OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
 * SUCH DAMAGE.*
 */

/*
 * Copyright 2019 Joyent, Inc.
 */

/*
 * Module to perform multi-part upload commits.
 *
 * The multipart upload is defined in RFD 65. At its core, the job here is to
 * take a list of files, concatenate them, and then return the md5 checksum to
 * the client.
 *
 * This is implemented as an nginx module, which is currently compiled into
 * nginx. As part of nginx, there are two main things that we define:
 *
 * 1. A function which initializes and inserts us into the request handling
 * array (mpu_init()).
 *
 * 2. Functions to manipulate the configuration options and a table of options
 * (mpu_create_loc_conf() and mpu_merge_loc_conf()).
 *
 * nginx's configuration is logically broken down into different name spaces
 * based on what it influences. The MPU module focuses on the 'location' level,
 * which corresponds to the 'location' keyword, which is meant to refer to a
 * section of the server's HTTP name space. When nginx is started up, it invokes
 * our two configuration functions for different portions of the configuration
 * tree. A user is required to set 'mpu_enabled', for a given section to be
 * relevant for MPUs. All URLs under a given location will be used for MPU
 * processing.
 *
 * As part of the mpu_init() function, this module inserts its own function into
 * the request processing pipeline. This function, mpu_handler(), is called for
 * most HTTP requets. If this function returns NGX_DECLINED, then we move onto
 * the next phase of the pipeline. Otherwise, if we return that we're handling
 * this request, nginx will wait for us to finish the request. Finally, if we
 * return an error, then nginx will return an error to the client immediately.
 *
 * If the 'mpu_enabled' flag is not set for the given location which the URI
 * maps to, then we'll always return NGX_DECLINED, so that it is handled by
 * another part of the system. This is how the dav module or the basic HTTP get
 * part works.
 *
 * If 'mpu_enabled' has been set to on, then we proceed to process the request
 * as an MPU. This happens in a few high-level steps:
 *
 * 1. Receive the entire MPU body
 * 2. Read in and parse the entire MPU body
 * 3. Validate the MPU JSON payload
 * 4. Perform the actual copy and rename operations (if needed)
 * 5. Return the calculated md5 sum to the user
 *
 * Because we have to read, checksum, and write each part of an MPU to a
 * destination temporary file, there are some gotchas with how we're performing
 * this. By default, nginx has a single thread that processes an event loop.
 * Doing all these file operations does not really fit into the classic event
 * loop model. To deal with that, all of these blocking operations happen inside
 * a specifically configured thread pool.
 *
 * More concretely, step 1 happens on the event loop. We ask nginx for the
 * entire body. This body gets streamed to disk asynchronously by nginx. We ask
 * nginx to stream this body asynchronously by calling the
 * ngx_http_read_client_request_body() function and specifying a callback when
 * it's done. Once we have kicked off this asynchronous operation, it is up to
 * us to tell nginx when we're done with the request. To do that we need to call
 * ngx_http_finalize_request() which vectors control to mpu_post_body() on
 * completion.
 *
 * Once the body has been read successfully by nginx, we start a thread pool job
 * by calling ngx_thread_task_post(). The thread pool allows us to specify two
 * functions. One function to run on the thread pool (mpu_task_handler()) and a
 * second function to run on the event loop (the main thread), when the thread
 * pool job is done (mpu_post_thread()). We can only call back into the nginx
 * HTTP handling code when we're on the main thread. So we can't call
 * ngx_http_finalize_request() or anything related until we reach
 * mpu_post_thread().
 *
 * The thread pool job (mpu_post_handler()), reads that file into memory, parses
 * the JSON blob, and then proceeds to execute the MPU after validating it. It's
 * worth pointing out that the reason we stream this to disk and then read it in
 * is because that allows us to have a single buffer which can be parsed for the
 * MPU blob, which is capped at 512K as part of configuration. nginx may be
 * convinced to keep it in memory, but it may only end up as a series of chained
 * buffers. Unfortunately, the supporting libraries in use don't support
 * disparate, chained buffers and thus we need to construct a single buffer with
 * the contents of the MPU JSON body.
 *
 * In the thread pool, we check to see if the file already exists. If it does,
 * we move on to reading its checksum and verifying the file size matches what
 * we expect from the request. If the file has already been constructed and the
 * checksum matches, we don't try to reconstruct the file. This is necessary
 * because a request can be resubmitted and the components making up the file
 * may have already been deleted after a previous submission.
 *
 * If the output file does not exist, we do all of our work in a temporary file.
 * We read in and calculate a running md5 checksum of the object and write out
 * the files in a cat(1)-like fashion. Once that's been assembled, we verify the
 * size of the file and the checksum against the request data. If everything
 * appears valid, we'll fsync and rename.
 *
 * Once this is done, we leave the thread pool and return to the main nginx
 * event loop.  From that context, we'll put together the response information
 * and give that to the client, finishing this request.
 *
 * Misc. Implementation Notes
 * --------------------------
 *
 *  - nginx strings are never null-terminated, they're kept as a pair of
 *    pointers pointing to the start and end of the string. This means that
 *    routines like ngx_snprintf() and co. do not append the '\0' character as
 *    you might expect. If you're expecting things to be null terminated, use
 *    libc routines. A side effect of this is that ngx_*printf cannot be used to
 *    do overflow detection. In general, when performing overflow detection, use
 *    snprintf(3C).
 *
 *  - The nginx memory allocation routines have different lifetimes. The
 *    ngx_alloc() and ngx_free() routines are like malloc and free, it is the
 *    responsibility of this module to keep track of them. However, the
 *    ngx_pcalloc() allocates memory tied to the lifetime of the HTTP request.
 *    As long as the allocation is less than a page in size, it will be freed as
 *    part of the request terminating. Note, some things like
 *    ngx_thread_task_alloc() use this as part of their request.
 *
 *  - If additional allocations are required for some reason, the ngx_pcalloc()
 *    family of functions should be used for small allocations that are tied to
 *    the request life cycle.
 *
 *  - Once we reach the point where we are running on a thread pool, all errors
 *    should go through mpu_set_error() which take care of making sure that both
 *    our internal request structure metadata is correct and that the error is
 *    properly logged and recorded for replying to the client.
 *
 *  - nginx has the ability to automatically clean up and remove temporary
 *    files. We use this for the temporary file that represents the MPU JSON
 *    payload. However, we do not use this for the actual temporary file that we
 *    end up creating for renaming. The reason for this is that that temporary
 *    file will ultimately be renamed when successful. If it's successfully
 *    renamed, then we don't want to unlink the file, as nginx could have
 *    generated another temporary file with the same name. Instead, we manually
 *    clean that up.
 *
 *  - For each location entry, nginx calls our initialize function,
 *    mpu_create_loc_conf(), and then it merges data between subsequent levels
 *    with the mpu_merge_loc_conf() function. The _create_ function should
 *    always initialize everything to indicate that values aren't set.
 */

#include <ngx_config.h>
#include <ngx_core.h>
#include <ngx_http.h>
#include <ngx_thread_pool.h>
#include <ngx_md5.h>
#include <strings.h>
#include <libgen.h>
#include <stdarg.h>
#include <sys/debug.h>
#include <atomic.h>

#include "deps/json-nvlist.h"
#include "deps/jsonemitter.h"

/*
 * Be paranoid about making sure we have 64-bit interfaces.
 */
#if defined(__i386) && !defined(_FILE_OFFSET_BITS)
#error "32-bit build without _FILE_OFFSET_BITS"
#endif

#if defined(__i386) && _FILE_OFFSET_BITS != 64
#error "incorrect value for _FILE_OFFSET_BITS"
#endif

#ifndef	MIN
#define	MIN(a, b)	((a) < (b) ? (a) : (b))
#endif

/*
 * This module attempts to be agnostic to the md5 implementation in use;
 * however, it does expect that the headers define MD5_DIGEST_LENGTH. This is
 * currently defined by both the system headers on illumos and OpenSSL. While we
 * could define it ourselves, if it's missing, that's a sign that we should
 * figure out what md5 implementation we're actually using.
 */
#ifndef	MD5_DIGEST_LENGTH
#error "md5 implementation headers missing common MD5_DIGEST_LENGTH macro"
#endif

/*
 * We read up to 512k into a buffer for the commit POST request body. Anything
 * larger is thrown out. This number is based on the idea that we could have
 * have 10k UUIDs in parts, which would translate into around 400k characters.
 */
#define	MPU_COMMIT_POST_SIZE	(512 * 1024)

/*
 * We use 2 MB buffer sizes to try and maximize the amount of heap usage.
 */
#define	MPU_COMMIT_RW_SIZE	(2 * 1024 * 1024)

/*
 * This is the length of C buffer that encodes a base64 encoded value.
 */
#define	MPU_MD5_B64_LEN	(ngx_base64_encoded_length(MD5_DIGEST_LENGTH) + 1)

/*
 * This is the size of our static error buffer in the error handler.
 */
#define	MPU_ERR_BUF_LEN		512

typedef struct {
	ngx_flag_t		mcl_enabled;
	ngx_thread_pool_t	*mcl_pool;
	ngx_str_t		mcl_root;
} mpu_loc_conf_t;

/*
 * This is a per-request structure that is allocated as part of the thread pool
 * task allocation.
 */
typedef struct {
	ngx_http_request_t	*mpcr_http;
	ngx_thread_task_t	*mpcr_task;
	char			*mpcr_buf;
	size_t			mpcr_buflen;
	const char		*mpcr_account;
	const char		*mpcr_objid;
	const char		*mpcr_root;
	const char		*mpcr_req_md5;
	int64_t			mpcr_nbytes;
	ngx_int_t		mpcr_status;
	ngx_buf_t		mpcr_ngx_buf;
	ngx_md5_t		mpcr_md5;
	unsigned char		mpcr_md5_buf[MD5_DIGEST_LENGTH];
	char			mpcr_md5_b64[MPU_MD5_B64_LEN];
	char			mpcr_error[MPU_ERR_BUF_LEN];
} mpu_request_t;

/* Forwards */
ngx_module_t ngx_http_mpu_commit_module;

static const char *mpu_content = "application/json";

/*
 * Stat to keep track of times we can't schedule data on a thread pool.
 */
volatile uint64_t mpu_overloads;

/*
 * Basically we want a way to indicate in a few functions that the output file
 * already exists. Hence the MPU_EALREADY.
 */
typedef enum {
	MPU_FAILURE	= -1,
	MPU_SUCCESS	= 0,
	MPU_EALREADY	= 1
} mpu_status_t;

static char *
mpu_set_pool(ngx_conf_t *cf, ngx_command_t *cmd, void *conf)
{
	ngx_str_t name;
	ngx_thread_pool_t *tp;
	ngx_str_t  *value;
	mpu_loc_conf_t *mcl = conf;

	value = cf->args->elts;
	name.len = value[1].len;
	name.data = value[1].data;

	tp = ngx_thread_pool_add(cf, &name);
	if (tp == NULL) {
		ngx_conf_log_error(NGX_LOG_EMERG, cf, 0,
		    "invalid thread pool specified for MPU commit");
		return (NGX_CONF_ERROR);
	}
	mcl->mcl_pool = tp;

	return (NGX_CONF_OK);
}

static ngx_command_t  mpu_commands[] = {
	{ ngx_string("mpu_enabled"),
	    NGX_HTTP_LOC_CONF|NGX_CONF_TAKE1,
	    ngx_conf_set_flag_slot,
	    NGX_HTTP_LOC_CONF_OFFSET,
	    offsetof(mpu_loc_conf_t, mcl_enabled),
	    NULL },

	{ ngx_string("mpu_pool"),
	    NGX_HTTP_LOC_CONF|NGX_CONF_TAKE1,
	    mpu_set_pool,
	    NGX_HTTP_LOC_CONF_OFFSET,
	    0, NULL },

	{ ngx_string("mpu_root"),
	    NGX_HTTP_LOC_CONF|NGX_CONF_TAKE1,
	    ngx_conf_set_str_slot,
	    NGX_HTTP_LOC_CONF_OFFSET,
	    offsetof(mpu_loc_conf_t, mcl_root),
	    NULL },

    ngx_null_command
};

static void *
mpu_create_loc_conf(ngx_conf_t *cf)
{
	mpu_loc_conf_t *conf;

	conf = ngx_pcalloc(cf->pool, sizeof (mpu_loc_conf_t));
	if (conf == NULL) {
		return (NULL);
	}

	conf->mcl_enabled = NGX_CONF_UNSET;
	conf->mcl_pool = NGX_CONF_UNSET_PTR;
	conf->mcl_root.len = 0;
	conf->mcl_root.data = NULL;

	return (conf);
}

static char *
mpu_merge_loc_conf(ngx_conf_t *cf, void *parent, void *child)
{
	mpu_loc_conf_t *prev = parent;
	mpu_loc_conf_t *conf = child;

	ngx_conf_merge_value(conf->mcl_enabled, prev->mcl_enabled, 0);
	ngx_conf_merge_ptr_value(conf->mcl_pool, prev->mcl_pool,
	    NGX_CONF_UNSET_PTR);
	ngx_conf_merge_str_value(conf->mcl_root, prev->mcl_root, "");
	if (conf->mcl_enabled == 1 && conf->mcl_pool == NGX_CONF_UNSET_PTR) {
		ngx_conf_log_error(NGX_LOG_EMERG, cf, 0,
		    "missing required MPU setting: \"mpu_pool\"");
		return (NGX_CONF_ERROR);
	}
	if (conf->mcl_enabled == 1 && (conf->mcl_root.len == 0 ||
	    (conf->mcl_root.len == 0 && conf->mcl_root.data[0] == '\0'))) {
		ngx_conf_log_error(NGX_LOG_EMERG, cf, 0,
		    "missing required MPU setting \"mpu_root\"");
		return (NGX_CONF_ERROR);
	}
	return (NGX_CONF_OK);
}

/*
 * Create a minimal form of the error message that occurred. This is used in
 * cases where we have hit internal memory issues and we know that the string
 * being embedded in the JSON does not require escaping.
 */
static void
mpu_set_error_fallback(mpu_request_t *mpcr, const char *code)
{
	(void) snprintf(mpcr->mpcr_error, sizeof (mpcr->mpcr_error),
	    "{ \"code\": \"%s\" }", code);
}

/*
 * We always render the string that we want to pass to nginx ourselves so that
 * way we can include strerror of errno or not as we need. Format this once for
 * nginx and then once as JSON.
 */
static void
mpu_set_error(mpu_request_t *mpcr, int status, int err, char *format, ...)
{
	int off, ret;
	va_list ap;
	const char *code;
	json_emit_t *jse;
	char buf[MPU_ERR_BUF_LEN];

	ngx_http_request_t *r = mpcr->mpcr_http;
	mpcr->mpcr_status = status;

	va_start(ap, format);
	ret = vsnprintf(buf, sizeof (buf), format, ap);
	ngx_log_error(NGX_LOG_ERR, r->connection->log, err, "%s", buf);
	mpcr->mpcr_error[0] = '\0';


	/*
	 * Translate the HTTP status code into an error that seems appropriate
	 * for the given issue that matches what Muskie and co. are likely to
	 * use.
	 */
	switch (status) {
	case NGX_HTTP_BAD_REQUEST:
	case NGX_HTTP_CONFLICT:
		code = "BadRequestError";
		break;
	case NGX_HTTP_INTERNAL_SERVER_ERROR:
	default:
		code = "InternalError";
		break;
	}

	if (ret == -1 || ret >= MPU_ERR_BUF_LEN) {
		mpu_set_error_fallback(mpcr, code);
		va_end(ap);
		return;
	}

	if (err != 0) {
		off = snprintf(buf + ret, sizeof (buf) - ret, ": %s",
		    strerror(err));
		if (off == -1 || off + ret >= MPU_ERR_BUF_LEN) {
			mpu_set_error_fallback(mpcr, code);
			va_end(ap);
			return;
		}
	}
	va_end(ap);

	jse = json_create_fixed_string(mpcr->mpcr_error,
	    sizeof (mpcr->mpcr_error));
	if (jse == NULL) {
		mpu_set_error_fallback(mpcr, code);
		return;
	}
	json_object_begin(jse, NULL);
	json_utf8string(jse, "code", code);
	json_utf8string(jse, "message", buf);
	json_object_end(jse);
	if (json_get_error(jse, buf, sizeof (buf)) != JSE_NONE) {
		mpu_set_error_fallback(mpcr, code);
		json_fini(jse);
		return;
	}
	json_fini(jse);
}

/*
 * Read in a request from the nginx temporary file
 */
static boolean_t
mpu_readin_request(mpu_request_t *mpcr, nvlist_t **nvl)
{
	ssize_t ret;
	off_t off;
	struct stat st;
	int fd;
	nvlist_parse_json_error_t nverr;
	ngx_http_request_t *r;
	char *buf = mpcr->mpcr_buf;

	r = mpcr->mpcr_http;
	fd = r->request_body->temp_file->file.fd;
	if (ngx_fd_info(fd, &st) != 0) {
		VERIFY3S(errno, !=, EFAULT);
		mpu_set_error(mpcr, NGX_HTTP_INTERNAL_SERVER_ERROR, ngx_errno,
		    "failed to stat mpu upload file %s",
		    r->request_body->temp_file->file.name);
		return (B_FALSE);
	}

	if (st.st_size >= MPU_COMMIT_POST_SIZE) {
		mpu_set_error(mpcr, NGX_HTTP_BAD_REQUEST, 0,
		    "POST body size was %lld, larger than max of %d",
		    st.st_size, MPU_COMMIT_POST_SIZE);
		return (B_FALSE);
	}

	buf[0] = '\0';
	off = 0;
	do {
		size_t toread = MIN((size_t)st.st_size, (size_t)mpcr->mpcr_buflen);

		/*
		 * Use pread(2), we don't know where nginx has left off reading
		 * this file nor do we know where it expects to be.
		 */
		ret = pread(fd, buf + off, toread, off);
		if (ret < 0) {
			VERIFY3S(errno, !=, EFAULT);
			mpu_set_error(mpcr, NGX_HTTP_INTERNAL_SERVER_ERROR,
			    ngx_errno, "failed to pread %lld bytes from fd %d "
			    "at off %lld from post body", toread, fd, off);
			return (B_FALSE);
		}
		off += ret;
		st.st_size -= toread;

		if (ret == 0 && st.st_size != 0) {
			mpu_set_error(mpcr, NGX_HTTP_INTERNAL_SERVER_ERROR, 0,
			    "hit EOF after reading %lld bytes, but still had "
			    "%lld remaining", off, st.st_size);
			return (B_FALSE);

		}
	} while (st.st_size > 0);

	ret = nvlist_parse_json(buf, off, nvl, NVJSON_FORCE_INTEGER, &nverr);

	if (ret != 0) {
		mpu_set_error(mpcr, NGX_HTTP_BAD_REQUEST, nverr.nje_errno,
		    "mpu commit encountered invalid JSON at pos %ld: %s",
		    nverr.nje_pos, nverr.nje_message);
		return (B_FALSE);
	}

	return (B_TRUE);
}

static boolean_t
mpu_alloc_tmpfile(mpu_request_t *mpcr, ngx_file_t *file)
{
	ngx_http_core_loc_conf_t *clcf;
	ngx_http_request_t *r = mpcr->mpcr_http;

	clcf = ngx_http_get_module_loc_conf(r, ngx_http_core_module);

	file->fd = NGX_INVALID_FILE;
	file->log = r->connection->log;

	if (ngx_create_temp_file(file, clcf->client_body_temp_path, r->pool,
	    1, 0, 0660) != NGX_OK) {
		mpu_set_error(mpcr, NGX_HTTP_INTERNAL_SERVER_ERROR, 0,
		    "failed to create temporary file for MPU output");
		return (B_FALSE);
	}

	return (B_TRUE);
}

/*
 * Verify that the client md5 checksum, if present, matches what we have
 * generated for our file. Note, we always check the base64 encoded md5
 * checksum. See the notes in ngx_htp_dav_module.c for more on why we do it this
 * way.
 */
static boolean_t
mpu_verify_md5(mpu_request_t *mpcr)
{
	if (mpcr->mpcr_req_md5 == NULL)
		return (B_TRUE);

	if (strcmp(mpcr->mpcr_req_md5, mpcr->mpcr_md5_b64) != 0) {
		/*
		 * We use a 469 which is what we use elsewhere in mako.
		 */
		mpu_set_error(mpcr, 469, 0, "md5 checksums mismatched: client "
		    "b64 md5 is %s, calculated value is %s", mpcr->mpcr_req_md5,
		    mpcr->mpcr_md5_b64);
		return (B_FALSE);
	}

	return (B_TRUE);
}

/*
 * Convert the md5 sum to a b64 value
 */
static boolean_t
mpu_convert_md5(mpu_request_t *mpcr)
{
	ngx_str_t md5, b64;

	md5.data = (u_char *)mpcr->mpcr_md5_buf;
	md5.len = MD5_DIGEST_LENGTH;
	b64.data = (u_char *)mpcr->mpcr_md5_b64;
	ngx_encode_base64(&b64, &md5);
	if (b64.len != ngx_base64_encoded_length(MD5_DIGEST_LENGTH)) {
		mpu_set_error(mpcr, NGX_HTTP_INTERNAL_SERVER_ERROR, 0,
		    "MD5 b64 encoding did not result in proper length");
		return (B_FALSE);
	}
	b64.data[b64.len] = '\0';

	return (B_TRUE);
}

/*
 * This function calculates the md5 sum of an existing output file. We must
 * include the md5 for every successful create, thus we must calculate this even
 * for a file that already exists. Recall, we could have generated an output
 * file, but crashed before the caller got a successful response, hence we owe
 * them a computed md5 value.
 */
static boolean_t
mpu_determine_output_md5(mpu_request_t *mpcr, int fd)
{
	/*
	 * It's possible we have already started calculating the md5 sum of the
	 * object before this function is called, so we need to reinitialize the
	 * state of the md5 sum before starting from the beginning of the object.
	 */
	ngx_md5_init(&mpcr->mpcr_md5);

	for (;;) {
		ssize_t ret;

		ret = read(fd, mpcr->mpcr_buf, mpcr->mpcr_buflen);
		if (ret == 0)
			break;
		if (ret < 1) {
			VERIFY3S(errno, !=, EFAULT);
			mpu_set_error(mpcr, NGX_HTTP_INTERNAL_SERVER_ERROR,
			    errno, "failed to read from input file, fd %d", fd);
			return (B_FALSE);
		}

		ngx_md5_update(&mpcr->mpcr_md5, mpcr->mpcr_buf, ret);
	}

	ngx_md5_final(mpcr->mpcr_md5_buf, &mpcr->mpcr_md5);
	if (!mpu_convert_md5(mpcr))
		return (B_FALSE);

	if (!mpu_verify_md5(mpcr))
		return (B_FALSE);

	return (B_TRUE);
}

/*
 * Verify that the output file exists and if so, check that both the size and
 * md5 sum match. Note, we must also fsync the parent directory of this file.
 * There's a potential race condition where we get here after the rename
 * succeeds, but before the fsync of the parent directory completes. In that
 * case, we could complete this before the rename was guaranteed via fsync(2).
 * That would end up causing us to incorrectly acknowledge this as visible if
 * the system crashed before that fsync completed. As such, we do it here.
 */
static mpu_status_t
mpu_check_exists(mpu_request_t *mpcr)
{
	int fd, ret;
	char outfile[PATH_MAX];
	struct stat sb;

	if ((ret = snprintf(outfile, sizeof (outfile), "%s/%s/%s",
	    mpcr->mpcr_root, mpcr->mpcr_account, mpcr->mpcr_objid)) >=
	    PATH_MAX) {
		mpu_set_error(mpcr, NGX_HTTP_INTERNAL_SERVER_ERROR, 0,
		    "failed to assemble mpu output file, overflowed internal "
		    "snprintf buffer, needed %d bytes, had %d", ret,
		    sizeof (outfile));
		return (MPU_FAILURE);
	}

	if ((fd = ngx_open_file(outfile, O_RDONLY | O_NOCTTY, NGX_FILE_OPEN,
	    0660)) < 0) {
		VERIFY3S(errno, !=, EFAULT);
		if (errno == ENOENT)
			return (MPU_SUCCESS);
		mpu_set_error(mpcr, NGX_HTTP_INTERNAL_SERVER_ERROR, ngx_errno,
		    "failed to open output file %s", outfile);
		return (MPU_FAILURE);
	}

	if (ngx_fd_info(fd, &sb) != 0) {
		VERIFY3S(errno, !=, EFAULT);
		mpu_set_error(mpcr, NGX_HTTP_INTERNAL_SERVER_ERROR, ngx_errno,
		    "failed to stat output file %s", outfile);

		(void) ngx_close_file(fd);
		return (MPU_FAILURE);
	}

	if (sb.st_size != mpcr->mpcr_nbytes) {
		(void) ngx_close_file(fd);

		mpu_set_error(mpcr, NGX_HTTP_CONFLICT, 0,
		    "size of MPU output file %s is actually %lld, expected "
		    "%lld", outfile, sb.st_size, mpcr->mpcr_nbytes);
		return (MPU_FAILURE);
	}

	ret = mpu_determine_output_md5(mpcr, fd);
	(void) ngx_close_file(fd);
	if (ret == B_FALSE) {
		return (MPU_FAILURE);
	}

	/*
	 * fsync(2) the parent directory.
	 */
	(void) snprintf(outfile, sizeof (outfile), "%s/%s", mpcr->mpcr_root,
	    mpcr->mpcr_account);
	fd = ngx_open_file(outfile, O_RDONLY | O_NOCTTY, NGX_FILE_OPEN, 0660);
	if (fd < 0) {
		VERIFY3S(errno, !=, EFAULT);
		mpu_set_error(mpcr, NGX_HTTP_INTERNAL_SERVER_ERROR, errno,
		    "failed to open output directory %s for syncing",
		    outfile);
		return (MPU_FAILURE);
	}

	if (fsync(fd) != 0) {
		mpu_set_error(mpcr, NGX_HTTP_INTERNAL_SERVER_ERROR, errno,
		    "failed to fsync parent directory %s", outfile);
		(void) ngx_close_file(fd);
		return (MPU_FAILURE);
	}
	(void) ngx_close_file(fd);

	return (MPU_EALREADY);
}

static boolean_t
mpu_append_file(mpu_request_t *mpcr, int fromfd, int tofd)
{
	for (;;) {
		ssize_t ret, towrite;
		off_t off;

		ret = read(fromfd, mpcr->mpcr_buf, mpcr->mpcr_buflen);
		if (ret == 0)
			break;
		if (ret < 0) {
			VERIFY3S(errno, !=, EFAULT);
			mpu_set_error(mpcr, NGX_HTTP_INTERNAL_SERVER_ERROR,
			    errno, "failed to read from input file, fd %d",
			    fromfd);
			return (B_FALSE);
		}

		ngx_md5_update(&mpcr->mpcr_md5, mpcr->mpcr_buf, ret);

		towrite = ret;
		off = 0;
		do {
			ret = write(tofd, mpcr->mpcr_buf + off, towrite);
			if (ret < 0) {
				VERIFY3S(errno, !=, EFAULT);
				mpu_set_error(mpcr,
				    NGX_HTTP_INTERNAL_SERVER_ERROR, errno,
				    "failed to write to output file, fd %d",
				    tofd);
				return (B_FALSE);
			}
			towrite -= ret;
			off += ret;
		} while (towrite > 0);
	}

	return (B_TRUE);
}

/*
 * By the time this function finishes, the temporary input file must have been
 * renamed or removed. If this module does not remove the temporary file, then
 * nothing will and we can end up wasting space. Other functions don't have this
 * problem, as the caller knows enough to be able to always clean things up on
 * failure.
 */
static boolean_t
mpu_rename(mpu_request_t *mpcr, ngx_file_t *infile)
{
	int ret, dirfd;
	char outfile[PATH_MAX];
	ngx_http_request_t *r;

	r = mpcr->mpcr_http;
	if ((ret = snprintf(outfile, sizeof (outfile), "%s/%s/%s",
	    mpcr->mpcr_root, mpcr->mpcr_account, mpcr->mpcr_objid)) >=
	    PATH_MAX) {
		mpu_set_error(mpcr, NGX_HTTP_INTERNAL_SERVER_ERROR, 0,
		    "attempt to create path for output file with account "
		    "%s and object %s overflowed internal buffer, needed "
		    "%d bytes, had %d", mpcr->mpcr_account, mpcr->mpcr_objid,
		    ret, PATH_MAX);
		if (ngx_delete_file(infile->name.data) == NGX_FILE_ERROR) {
			VERIFY3S(errno, !=, EFAULT);
			ngx_log_error(NGX_LOG_ERR, r->connection->log, errno,
			    "failed to remove temporary file %s after size "
			    "check failed", infile->name.data);
		}
		return (B_FALSE);
	}

	if (fsync(infile->fd) != 0) {
		mpu_set_error(mpcr, NGX_HTTP_INTERNAL_SERVER_ERROR, errno,
		    "failed to fsync temporary file %s", infile->name.data);
		if (ngx_delete_file(infile->name.data) == NGX_FILE_ERROR) {
			VERIFY3S(errno, !=, EFAULT);
			ngx_log_error(NGX_LOG_ERR, r->connection->log, errno,
			    "failed to remove temporary file %s after size "
			    "check failed", infile->name.data);
		}
		return (B_FALSE);
	}

	if (rename((char *)infile->name.data, outfile) != 0) {
		VERIFY3S(errno, !=, EFAULT);
		mpu_set_error(mpcr, NGX_HTTP_INTERNAL_SERVER_ERROR, errno,
		    "failed to rename temporary file %s to %s",
		    infile->name.data, outfile);
		if (ngx_delete_file(infile->name.data) == NGX_FILE_ERROR) {
			VERIFY3S(errno, !=, EFAULT);
			ngx_log_error(NGX_LOG_ERR, r->connection->log, errno,
			    "failed to remove temporary file %s after size "
			    "check failed", infile->name.data);
		}
		return (B_FALSE);
	}

	if ((dirfd = ngx_open_file(dirname(outfile), O_RDONLY|O_NOCTTY,
	    NGX_FILE_OPEN, 0660)) < 0) {
		VERIFY3S(errno, !=, EFAULT);
		mpu_set_error(mpcr, NGX_HTTP_INTERNAL_SERVER_ERROR, errno,
		    "failed to open output directory %s for syncing",
		    outfile);
		return (B_FALSE);
	}

	if (fsync(dirfd) != 0) {
		mpu_set_error(mpcr, NGX_HTTP_INTERNAL_SERVER_ERROR, errno,
		    "failed to fsync parent directory %s", outfile);
		(void) ngx_close_file(dirfd);
		return (B_FALSE);
	}
	(void) ngx_close_file(dirfd);

	return (B_TRUE);
}

/*
 * Go through and make sure that every key is valid and that it fits inside of
 * our internal buffers.
 */
static boolean_t
mpu_check_parts(mpu_request_t *mpcr,
    const char *account, nvlist_t *parts, uint_t nparts)
{
	uint32_t i;

	for (i = 0; i < nparts; i++) {
		char key[64];
		char *file;
		char path[PATH_MAX];
		int ret;

		(void) snprintf(key, sizeof (key), "%d", i);
		if (nvlist_lookup_string(parts, key, &file) != 0) {
			mpu_set_error(mpcr, NGX_HTTP_BAD_REQUEST, 0,
			    "told %d parts exist, but part %d is not a valid "
			    "string", nparts, i);
			return (B_FALSE);
		}

		/*
		 * Explicitly forbid the presence of the characters '/' and '.'
		 * in the string to help deal with someone trying to path escape
		 * via the construction below.
		 */
		if (strchr(file, '.') != NULL || strchr(file, '/') != NULL) {
			mpu_set_error(mpcr, NGX_HTTP_BAD_REQUEST, 0,
			    "found illegal character, '.' or '/' in part %d "
			    "name", i);
			return (B_FALSE);
		}

		if ((ret = snprintf(path, sizeof (path), "%s/%s/%s",
		    mpcr->mpcr_root, account, file)) >= PATH_MAX) {
			mpu_set_error(mpcr, NGX_HTTP_BAD_REQUEST, 0,
			    "attempt to create path for part %d with account "
			    "%s and part %s overflowed internal buffer, needed "
			    "%d bytes, had %d", account, file, ret, PATH_MAX);
			return (B_FALSE);
		}
	}

	return (B_TRUE);
}

static mpu_status_t
mpu_nvl_append_parts(mpu_request_t *mpcr, nvlist_t *parts, uint32_t nparts,
    ngx_file_t *tmpfile)
{
	uint32_t i;

	for (i = 0; i < nparts; i++) {
		char key[64];
		char *file;
		char path[PATH_MAX];
		int fd;
		boolean_t ret;

		(void) snprintf(key, sizeof (key), "%d", i);
		file = fnvlist_lookup_string(parts, key);
		(void) snprintf(path, sizeof (path), "%s/%s/%s",
		    mpcr->mpcr_root, mpcr->mpcr_account, file);

		fd = ngx_open_file(path, O_RDONLY|O_NOCTTY, NGX_FILE_OPEN,
		    0660);
		if (fd < 0) {
			int err = errno;
			VERIFY3S(errno, !=, EFAULT);
			/*
			 * We encountered a missing part. If we're missing a
			 * part, then that might indicate that we're racing with
			 * another commit. As such, we try to check if the
			 * output file exists and if so, leverage that.
			 */
			if (errno == ENOENT && mpu_check_exists(mpcr) ==
			    MPU_EALREADY) {
				return (MPU_EALREADY);
			}

			/*
			 * Clobber whatever error we may have encountered above
			 * with the actual thing we originally saw.
			 */
			mpu_set_error(mpcr, NGX_HTTP_BAD_REQUEST, err,
			    "failed to open file %s for part %d", path, i);
			return (MPU_FAILURE);
		}

		ret = mpu_append_file(mpcr, fd, tmpfile->fd);
		(void) ngx_close_file(fd);
		if (ret == B_FALSE)
			return (MPU_FAILURE);
	}

	return (MPU_SUCCESS);
}

static void
mpu_cleanup_parts(mpu_request_t *mpcr, nvlist_t *parts, uint_t nparts)
{
	uint32_t i;
	ngx_http_request_t *r = mpcr->mpcr_http;

	for (i = 0; i < nparts; i++) {
		char key[64];
		char *file;
		char path[PATH_MAX];

		(void) snprintf(key, sizeof (key), "%d", i);
		file = fnvlist_lookup_string(parts, key);
		(void) snprintf(path, sizeof (path), "%s/%s/%s",
		    mpcr->mpcr_root, mpcr->mpcr_account, file);

		/*
		 * If we fail to remove a part, we're in a bad situation. In
		 * theory we've successfully created everything else about this
		 * request. Unfortunately, failing the request at this point is
		 * not going to be very helpful. Instead, we simply log, and
		 * move on. This means that these abandoned parts will need to
		 * be eventually cleaned up. We don't bother logging about
		 * ENOENT as that may be a natural state given races between
		 * multiple MPUs.
		 */
		if (unlink(path) != 0 && errno != ENOENT) {
			VERIFY3S(errno, !=, EFAULT);
			ngx_log_error(NGX_LOG_ERR, r->connection->log, errno,
			    "failed to remove file %s for part %d", path, i);
		}
	}

}

static boolean_t
mpu_verify_size(mpu_request_t *mpcr, ngx_file_t *fp)
{
	struct stat st;

	if (ngx_fd_info(fp->fd, &st) != 0) {
		mpu_set_error(mpcr, NGX_HTTP_INTERNAL_SERVER_ERROR, ngx_errno,
		    "failed to stat temporary file %s", fp->name.data);
		return (B_FALSE);
	}

	if (st.st_size != mpcr->mpcr_nbytes) {
		/*
		 * This is a bit of a tricky case, it's hard to say what the
		 * right error is. It could be that this module failed to
		 * assemble the output file correctly, or that what we were
		 * given didn't match. For now we opt to return a 409, as
		 * there's not necessarily a better option.
		 */
		mpu_set_error(mpcr, NGX_HTTP_CONFLICT, 0,
		    "assembled temporary file %s has size %lld bytes, request "
		    "specified %lld bytes", fp->name.data, st.st_size,
		    mpcr->mpcr_nbytes);
		return (B_FALSE);
	}

	return (B_TRUE);
}

/*
 * Validate the various parts of the MPU commit message and process them.
 */
static boolean_t
mpu_nvl_process(mpu_request_t *mpcr, nvlist_t *nvl)
{
	int ret;
	uint32_t nparts;
	int64_t version, nbytes;
	char *account, *objid, *req_md5;
	nvlist_t *parts;
	ngx_file_t tmpfile;
	ngx_http_request_t *r;

	r = mpcr->mpcr_http;
	if ((ret = nvlist_lookup_int64(nvl, "version", &version)) != 0) {
		mpu_set_error(mpcr, NGX_HTTP_BAD_REQUEST, ret,
		    "mpu commit JSON missing version");
		return (B_FALSE);
	}

	if (version != 1) {
		mpu_set_error(mpcr, NGX_HTTP_BAD_REQUEST, 0,
		    "mpu commit encountered unknown version: %lld", version);
		return (B_FALSE);
	}

	if ((ret = nvlist_lookup_int64(nvl, "nbytes", &nbytes)) != 0) {
		mpu_set_error(mpcr, NGX_HTTP_BAD_REQUEST, ret,
		    "mpu commit JSON missing nbytes");
		return (B_FALSE);
	}

	if (nbytes <= 0) {
		mpu_set_error(mpcr, NGX_HTTP_BAD_REQUEST, 0,
		    "mpu commit encountered invalid bytes: %lld", nbytes);
		return (B_FALSE);
	}

	if ((ret = nvlist_lookup_string(nvl, "account", &account)) != 0) {
		mpu_set_error(mpcr, NGX_HTTP_BAD_REQUEST, ret,
		    "mpu commit JSON missing account");
		return (B_FALSE);
	}

	if ((ret = nvlist_lookup_string(nvl, "objectId", &objid)) != 0) {
		mpu_set_error(mpcr, NGX_HTTP_BAD_REQUEST, ret,
		    "mpu commit JSON missing objectId");
		return (B_FALSE);
	}

	if ((ret = nvlist_lookup_nvlist(nvl, "parts", &parts)) != 0) {
		mpu_set_error(mpcr, NGX_HTTP_BAD_REQUEST, ret,
		    "mpu commit JSON missing parts");
		return (B_FALSE);
	}

	if (nvlist_lookup_boolean(parts, ".__json_array") != 0 ||
	    nvlist_lookup_uint32(parts, "length", &nparts) != 0) {
		mpu_set_error(mpcr, NGX_HTTP_BAD_REQUEST, 0,
		    "mpu commit JSON parts not parsed to a valid array");
		return (B_FALSE);
	}

	/*
	 * The request md5 value is optional, and should be a base64 encoded
	 * value.
	 */
	if (nvlist_lookup_string(nvl, "md5", &req_md5) == 0) {
		u_char buf[MD5_DIGEST_LENGTH];
		ngx_str_t dec, enc;

		if ((ret = strlen(req_md5)) != ngx_base64_encoded_length(16)) {
			mpu_set_error(mpcr, NGX_HTTP_BAD_REQUEST, 0,
			    "client md5 value is not properly b64 encoded, "
			    "expected %d bytes, got %d",
			    ngx_base64_encoded_length(16), ret);
			return (B_FALSE);
		}

		dec.data = buf;
		enc.data = (u_char *)req_md5;
		enc.len = ngx_base64_encoded_length(16);
		if (ngx_decode_base64(&dec, &enc) != NGX_OK) {
			mpu_set_error(mpcr, NGX_HTTP_BAD_REQUEST, 0,
			    "client md5 value is not properly b64 encoded, "
			    "failed to decode b64 value");
			return (B_FALSE);
		}

		mpcr->mpcr_req_md5 = req_md5;
	}

	if (strchr(account, '.') != NULL || strchr(account, '/') != NULL) {
		mpu_set_error(mpcr, NGX_HTTP_BAD_REQUEST, 0,
		    "found illegal character, '.' or '/' in account name");
		return (B_FALSE);
	}

	if (strchr(account, '.') != NULL || strchr(account, '/') != NULL) {
		mpu_set_error(mpcr, NGX_HTTP_BAD_REQUEST, 0,
		    "found illegal character, '.' or '/' in objid name");
		return (B_FALSE);
	}

	if (!mpu_check_parts(mpcr, account, parts, nparts))
		return (B_FALSE);

	mpcr->mpcr_account = account;
	mpcr->mpcr_objid = objid;
	mpcr->mpcr_nbytes = nbytes;

	ret = mpu_check_exists(mpcr);
	if (ret == MPU_FAILURE)
		return (B_FALSE);
	if (ret == MPU_EALREADY)
		goto cleanup;

	/*
	 * We'll validate the parts on the fly.
	 */
	bzero(&tmpfile, sizeof (tmpfile));
	if (!mpu_alloc_tmpfile(mpcr, &tmpfile))
		return (B_FALSE);

	if ((ret = mpu_nvl_append_parts(mpcr, parts, nparts, &tmpfile)) !=
	    MPU_SUCCESS) {
		/*
		 * While trying to append parts, we may have been unable to find
		 * a part. That could be because there was a bad specification
		 * or because there was just a race with something else that had
		 * correctly constructed the file. In either case we always
		 * unlink our temporary file.
		 */
		if (ngx_delete_file(tmpfile.name.data) == NGX_FILE_ERROR) {
			VERIFY3S(errno, !=, EFAULT);
			ngx_log_error(NGX_LOG_ERR, r->connection->log, errno,
			    "failed to remove temporary file %s after failing "
			    "to append parts", tmpfile.name.data);
		}

		if (ret == MPU_EALREADY)
			goto cleanup;
		return (B_FALSE);
	}

	if (!mpu_verify_size(mpcr, &tmpfile)) {
		if (ngx_delete_file(tmpfile.name.data) == NGX_FILE_ERROR) {
			VERIFY3S(errno, !=, EFAULT);
			ngx_log_error(NGX_LOG_ERR, r->connection->log, errno,
			    "failed to remove temporary file %s after size "
			    "check failed", tmpfile.name.data);
		}
		return (B_FALSE);
	}

	ngx_md5_final(mpcr->mpcr_md5_buf, &mpcr->mpcr_md5);

	if (!mpu_convert_md5(mpcr) || !mpu_verify_md5(mpcr)) {
		if (ngx_delete_file(tmpfile.name.data) == NGX_FILE_ERROR) {
			VERIFY3S(errno, !=, EFAULT);
			ngx_log_error(NGX_LOG_ERR, r->connection->log, errno,
			    "failed to remove temporary file %s after md5 "
			    "calc failed", tmpfile.name.data);
		}
		return (B_FALSE);
	}

	/*
	 * This function guarantees that the temporary file is either renamed or
	 * deleted.
	 */
	if (!mpu_rename(mpcr, &tmpfile))
		return (B_FALSE);

cleanup:
	mpu_cleanup_parts(mpcr, parts, nparts);

	return (B_TRUE);
}

/*
 * This is called in the context of a given MPU request. Here we must do the
 * heavy lifting of parsing the request and taking the actual actions.
 */
static void
mpu_task_handler(void *arg, ngx_log_t *log)
{
	nvlist_t *nvl;
	mpu_request_t *mpcr = arg;
	char *buf;

	buf = ngx_alloc(MPU_COMMIT_RW_SIZE, mpcr->mpcr_http->connection->log);
	if (buf == NULL) {
		mpcr->mpcr_status = NGX_HTTP_INTERNAL_SERVER_ERROR;
		return;
	}

	mpcr->mpcr_buf = buf;
	mpcr->mpcr_buflen = MPU_COMMIT_RW_SIZE;
	if (!mpu_readin_request(mpcr, &nvl)) {
		ngx_free(buf);
		mpcr->mpcr_buf = NULL;
		mpcr->mpcr_buflen = 0;
		return;
	}

	if (mpu_nvl_process(mpcr, nvl)) {
		mpcr->mpcr_status = NGX_HTTP_NO_CONTENT;
	}

	nvlist_free(nvl);
	ngx_free(buf);
	mpcr->mpcr_buf = NULL;
	mpcr->mpcr_buflen = 0;
}

/*
 * This function executes on the main thread after we have finished executing
 * our thread pool.
 */
static void
mpu_post_thread(ngx_event_t *ev)
{
	int ret;
	mpu_request_t *mpcr = ev->data;
	ngx_http_request_t *r = mpcr->mpcr_http;


	/*
	 * We need to clean up some state here. We initially set that
	 * we were blocked before we entered the thread pool. Now that we're
	 * finally done with the thread pool, remove that indication. After
	 * we've removed that indication, we need to call any write event
	 * handler. That handler may do nothing or if we tried to finalize the
	 * request while blocked, it will now take care of that action. Note,
	 * it's important that we do this before we call back and try to output
	 * data to nginx.
	 */
	r->main->blocked--;
	r->write_event_handler(r);

	r->headers_out.status = mpcr->mpcr_status;
	if (mpcr->mpcr_status == NGX_HTTP_NO_CONTENT) {
		ngx_table_elt_t *h;
		h = ngx_list_push(&r->headers_out.headers);
		if (h == NULL) {
			/*
			 * There's not much we can do at this point, just error
			 * out.
			 */
			ret = NGX_HTTP_INTERNAL_SERVER_ERROR;
			ngx_log_error(NGX_LOG_ERR, r->connection->log, 0,
			    "failed to allocate header for MD5");
		} else {
			h->hash = 1;
			ngx_str_set(&h->key, "X-Joyent-Computed-Content-MD5");
			h->value.data = (u_char *)mpcr->mpcr_md5_b64;
			h->value.len = strlen(mpcr->mpcr_md5_b64);
			r->headers_out.content_length_n = 0;
			ret = mpcr->mpcr_status;
		}
	} else {
		ngx_chain_t out;
		ngx_buf_t *b = &mpcr->mpcr_ngx_buf;

		ngx_str_set(&r->headers_out.content_type, mpu_content);
		r->headers_out.content_type.len = strlen(mpu_content);
		r->headers_out.content_length_n = strlen(mpcr->mpcr_error);

		b->pos = (u_char *)mpcr->mpcr_error;
		b->last = (u_char *)mpcr->mpcr_error +
		    r->headers_out.content_length_n;
		b->memory = 1;
		b->last_buf = 1;

		out.buf = b;
		out.next = NULL;
		ngx_http_send_header(r);
		ret = ngx_http_output_filter(r, &out);
	}

	ngx_http_finalize_request(r, ret);
	/*
	 * We must tell the event loop to move forward with this connection
	 * because we have run this in the thread pool and thus blocked some set
	 * of events on it.
	 */
	ngx_http_run_posted_requests(r->connection);
}

/*
 * This function executes on the event loop after the full body has been read by
 * nginx into an appropriate file.
 */
static void
mpu_post_body(ngx_http_request_t *r)
{
	ngx_thread_task_t *task;
	mpu_request_t *mpcr;
	mpu_loc_conf_t *conf;

	if (r->request_body == NULL) {
		ngx_log_error(NGX_LOG_CRIT, r->connection->log, 0, "mpu body "
		    "callback missing request body!");
		ngx_http_finalize_request(r, NGX_HTTP_INTERNAL_SERVER_ERROR);
		return;
	}

	if (r->request_body->temp_file == NULL) {
		ngx_log_error(NGX_LOG_CRIT, r->connection->log, 0, "mpu body "
		    "callback missing request temporary file!");
		ngx_http_finalize_request(r, NGX_HTTP_INTERNAL_SERVER_ERROR);
		return;
	}

	conf = ngx_http_get_module_loc_conf(r, ngx_http_mpu_commit_module);
	task = ngx_thread_task_alloc(r->pool, sizeof (mpu_request_t));
	if (task == NULL) {
		atomic_inc_64(&mpu_overloads);
		ngx_log_error(NGX_LOG_CRIT, r->connection->log, 0,
		    "failed to allcoate MPU request structure");
		ngx_http_finalize_request(r, NGX_HTTP_INTERNAL_SERVER_ERROR);
		return;
	}

	mpcr = task->ctx;
	mpcr->mpcr_http = r;
	mpcr->mpcr_task = task;
	mpcr->mpcr_root = (const char *)conf->mcl_root.data;
	mpcr->mpcr_status = NGX_HTTP_INTERNAL_SERVER_ERROR;
	ngx_md5_init(&mpcr->mpcr_md5);

	task->handler = mpu_task_handler;
	task->event.data = mpcr;
	task->event.handler = mpu_post_thread;

	if (ngx_thread_task_post(conf->mcl_pool, task) != NGX_OK) {
		ngx_log_error(NGX_LOG_CRIT, r->connection->log, 0,
		    "failed to submit taskq entry, limit likely exceeded");
		ngx_http_finalize_request(r, NGX_HTTP_INTERNAL_SERVER_ERROR);
		return;
	}

	/*
	 * Indicate that there is more going on in this request and that it
	 * should not be cleaned up automatically. This is required because our
	 * thread pool activity will be running asynchronously from this thread
	 * and once we return saying we've handled it, it will try and finalize
	 * the request.
	 */
	r->main->blocked++;

}

/*
 * This is the primary function that is called by nginx to handle a request.
 */
static ngx_int_t
mpu_handler(ngx_http_request_t *r)
{
	mpu_loc_conf_t *conf;

	conf = ngx_http_get_module_loc_conf(r, ngx_http_mpu_commit_module);
	if (conf->mcl_enabled != 1) {
		return (NGX_DECLINED);
	}

	if (r->method != NGX_HTTP_POST) {
		ngx_log_error(NGX_LOG_ERR, r->connection->log, 0,
		    "Only \"POST\" requests allowed");
		return (NGX_HTTP_NOT_ALLOWED);
	}

	r->request_body_in_file_only = 1;
	r->request_body_in_persistent_file = 1;
	r->request_body_in_clean_file = 1;
	r->request_body_file_group_access = 1;
	r->request_body_file_log_level = 0;

	return (ngx_http_read_client_request_body(r,
	    mpu_post_body));
}

static ngx_int_t
mpu_init(ngx_conf_t *cf)
{
	ngx_http_handler_pt	*h;
	ngx_http_core_main_conf_t  *cmcf;

	cmcf = ngx_http_conf_get_module_main_conf(cf, ngx_http_core_module);

	h = ngx_array_push(&cmcf->phases[NGX_HTTP_CONTENT_PHASE].handlers);
	if (h == NULL) {
		return (NGX_ERROR);
	}

	*h = mpu_handler;

	return (NGX_OK);
}

static ngx_http_module_t ngx_http_mpu_commit_module_ctx = {
	NULL,			/* preconfiguration */
	mpu_init,		/* postconfiguration */

	NULL,			/* create main configuration */
	NULL,			/* init main configuration */

	NULL,			/* create server configuration */
	NULL,			/* merge server configuration */

	mpu_create_loc_conf,	/* create location configuration */
	mpu_merge_loc_conf	/* merge location configuration */

};

ngx_module_t ngx_http_mpu_commit_module = {
	NGX_MODULE_V1,
	&ngx_http_mpu_commit_module_ctx,  /* module context */
	mpu_commands,			  /* module directives */
	NGX_HTTP_MODULE,		  /* module type */
	NULL,				  /* init master */
	NULL,				  /* init module */
	NULL,				  /* init process */
	NULL,				  /* init thread */
	NULL,				  /* exit thread */
	NULL,				  /* exit process */
	NULL,				  /* exit master */
	NGX_MODULE_V1_PADDING
};
