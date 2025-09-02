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
 * Copyright 2025 Edgecast Cloud LLC.
 */

/*
 * Module to perform multi-part upload commits for buckets-api (v2).
 *
 * This module extends the v1 MPU commit functionality to support the
 * bucket-aware storage layout used by manta-buckets-api. The key differences:
 *
 * 1. Enhanced JSON format with bucket metadata (owner, bucketId, objectHash)
 * 2. Flexible part paths supporting bucket-style storage layout
 * 3. Rich part metadata including ETags and individual part sizes
 * 4. Bucket-aware final object placement
 *
 * The v2 commit request format is:
 * {
 *   "version": 2,
 *   "nbytes": <total_size>,
 *   "owner": "<owner_uuid>",
 *   "bucketId": "<bucket_uuid>",
 *   "objectId": "<object_uuid>", 
 *   "objectHash": "<object_name_hash>",
 *   "uploadId": "<upload_id>",
 *   "parts": [
 *     {
 *       "partNumber": 1,
 *       "path": "/manta/v2/<owner>/<bucket>/<prefix>/.mpu-uploads/<id>/part.1",
 *       "etag": "\"d41d8cd98f00b204e9800998ecf8427e\"",
 *       "size": 5242880
 *     }
 *   ]
 * }
 *
 * Final objects are stored at:
 * /manta/v2/<owner>/<bucketId>/<objectId_prefix>/<objectId>,<objectHash>
 *
 * Like v1, this module uses nginx's thread pool for the heavy lifting of
 * reading, concatenating, and writing the final object to avoid blocking
 * the event loop.
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
 * We read up to 1MB for v2 commit requests to accommodate the richer
 * JSON format with full part paths and metadata.
 */
#define	MPU_V2_COMMIT_POST_SIZE	(1024 * 1024)

/*
 * We use 2 MB buffer sizes to try and maximize the amount of heap usage.
 */
#define	MPU_V2_COMMIT_RW_SIZE	(2 * 1024 * 1024)

/*
 * This is the length of C buffer that encodes a base64 encoded value.
 */
#define	MPU_V2_MD5_B64_LEN	(ngx_base64_encoded_length(MD5_DIGEST_LENGTH) + 1)

/*
 * This is the size of our static error buffer in the error handler.
 */
#define	MPU_V2_ERR_BUF_LEN		512

/*
 * Maximum number of parts allowed in a v2 multipart upload.
 */
#define	MPU_V2_MAX_PARTS		10000

typedef struct {
	ngx_flag_t		mcl_enabled;
	ngx_thread_pool_t	*mcl_pool;
	ngx_str_t		mcl_root;
} mpu_v2_loc_conf_t;

/*
 * Structure to hold individual part metadata for v2.
 */
typedef struct {
	ngx_uint_t		part_number;
	char			*path;
	char			*etag;
	int64_t			size;
} mpu_v2_part_t;

/*
 * This is a per-request structure that is allocated as part of the thread pool
 * task allocation.
 */
typedef struct {
	ngx_http_request_t	*mpcr_http;
	ngx_thread_task_t	*mpcr_task;
	char			*mpcr_buf;
	size_t			mpcr_buflen;
	const char		*mpcr_owner;
	const char		*mpcr_bucket_id;
	const char		*mpcr_object_id;
	const char		*mpcr_object_hash;
	const char		*mpcr_upload_id;
	const char		*mpcr_root;
	const char		*mpcr_req_md5;
	int64_t			mpcr_nbytes;
	ngx_uint_t		mpcr_nparts;
	mpu_v2_part_t		*mpcr_parts;
	ngx_int_t		mpcr_status;
	ngx_buf_t		mpcr_ngx_buf;
	ngx_md5_t		mpcr_md5;
	unsigned char		mpcr_md5_buf[MD5_DIGEST_LENGTH];
	char			mpcr_md5_b64[MPU_V2_MD5_B64_LEN];
	char			mpcr_error[MPU_V2_ERR_BUF_LEN];
} mpu_v2_request_t;

/* Forwards */
ngx_module_t ngx_http_mpu_v2_commit_module;

static const char *mpu_v2_content = "application/json";

/*
 * Stat to keep track of times we can't schedule data on a thread pool.
 */
volatile uint64_t mpu_v2_overloads;

/*
 * Basically we want a way to indicate in a few functions that the output file
 * already exists. Hence the MPU_V2_EALREADY.
 */
typedef enum {
	MPU_V2_FAILURE	= -1,
	MPU_V2_SUCCESS	= 0,
	MPU_V2_EALREADY	= 1
} mpu_v2_status_t;

static char *
mpu_v2_set_pool(ngx_conf_t *cf, ngx_command_t *cmd, void *conf)
{
	ngx_str_t name;
	ngx_thread_pool_t *tp;
	ngx_str_t  *value;
	mpu_v2_loc_conf_t *mcl = conf;

	value = cf->args->elts;
	name.len = value[1].len;
	name.data = value[1].data;

	tp = ngx_thread_pool_add(cf, &name);
	if (tp == NULL) {
		ngx_conf_log_error(NGX_LOG_EMERG, cf, 0,
		    "invalid thread pool specified for MPU v2 commit");
		return (NGX_CONF_ERROR);
	}
	mcl->mcl_pool = tp;

	return (NGX_CONF_OK);
}

static ngx_command_t  mpu_v2_commands[] = {
	{ ngx_string("mpu_v2_enabled"),
	    NGX_HTTP_LOC_CONF|NGX_CONF_TAKE1,
	    ngx_conf_set_flag_slot,
	    NGX_HTTP_LOC_CONF_OFFSET,
	    offsetof(mpu_v2_loc_conf_t, mcl_enabled),
	    NULL },

	{ ngx_string("mpu_v2_pool"),
	    NGX_HTTP_LOC_CONF|NGX_CONF_TAKE1,
	    mpu_v2_set_pool,
	    NGX_HTTP_LOC_CONF_OFFSET,
	    0, NULL },

	{ ngx_string("mpu_v2_root"),
	    NGX_HTTP_LOC_CONF|NGX_CONF_TAKE1,
	    ngx_conf_set_str_slot,
	    NGX_HTTP_LOC_CONF_OFFSET,
	    offsetof(mpu_v2_loc_conf_t, mcl_root),
	    NULL },

    ngx_null_command
};

static void *
mpu_v2_create_loc_conf(ngx_conf_t *cf)
{
	mpu_v2_loc_conf_t *conf;

	conf = ngx_pcalloc(cf->pool, sizeof (mpu_v2_loc_conf_t));
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
mpu_v2_merge_loc_conf(ngx_conf_t *cf, void *parent, void *child)
{
	mpu_v2_loc_conf_t *prev = parent;
	mpu_v2_loc_conf_t *conf = child;

	ngx_conf_merge_value(conf->mcl_enabled, prev->mcl_enabled, 0);
	ngx_conf_merge_ptr_value(conf->mcl_pool, prev->mcl_pool,
	    NGX_CONF_UNSET_PTR);
	ngx_conf_merge_str_value(conf->mcl_root, prev->mcl_root, "");
	if (conf->mcl_enabled == 1 && conf->mcl_pool == NGX_CONF_UNSET_PTR) {
		ngx_conf_log_error(NGX_LOG_EMERG, cf, 0,
		    "missing required MPU v2 setting: \"mpu_v2_pool\"");
		return (NGX_CONF_ERROR);
	}
	if (conf->mcl_enabled == 1 && (conf->mcl_root.len == 0 ||
	    (conf->mcl_root.len == 0 && conf->mcl_root.data[0] == '\0'))) {
		ngx_conf_log_error(NGX_LOG_EMERG, cf, 0,
		    "missing required MPU v2 setting \"mpu_v2_root\"");
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
mpu_v2_set_error_fallback(mpu_v2_request_t *mpcr, const char *code)
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
mpu_v2_set_error(mpu_v2_request_t *mpcr, int status, int err, char *format, ...)
{
	int off, ret;
	va_list ap;
	const char *code;
	json_emit_t *jse;
	char buf[MPU_V2_ERR_BUF_LEN];

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

	if (ret == -1 || ret >= MPU_V2_ERR_BUF_LEN) {
		mpu_v2_set_error_fallback(mpcr, code);
		va_end(ap);
		return;
	}

	if (err != 0) {
		off = snprintf(buf + ret, sizeof (buf) - ret, ": %s",
		    strerror(err));
		if (off == -1 || off + ret >= MPU_V2_ERR_BUF_LEN) {
			mpu_v2_set_error_fallback(mpcr, code);
			va_end(ap);
			return;
		}
	}
	va_end(ap);

	jse = json_create_fixed_string(mpcr->mpcr_error,
	    sizeof (mpcr->mpcr_error));
	if (jse == NULL) {
		mpu_v2_set_error_fallback(mpcr, code);
		return;
	}
	json_object_begin(jse, NULL);
	json_utf8string(jse, "code", code);
	json_utf8string(jse, "message", buf);
	json_object_end(jse);
	if (json_get_error(jse, buf, sizeof (buf)) != JSE_NONE) {
		mpu_v2_set_error_fallback(mpcr, code);
		json_fini(jse);
		return;
	}
	json_fini(jse);
}

/*
 * Read in a v2 request from the nginx temporary file
 */
static boolean_t
mpu_v2_readin_request(mpu_v2_request_t *mpcr, nvlist_t **nvl)
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
		mpu_v2_set_error(mpcr, NGX_HTTP_INTERNAL_SERVER_ERROR, ngx_errno,
		    "failed to stat mpu v2 upload file %s",
		    r->request_body->temp_file->file.name);
		return (B_FALSE);
	}

	if (st.st_size >= MPU_V2_COMMIT_POST_SIZE) {
		mpu_v2_set_error(mpcr, NGX_HTTP_BAD_REQUEST, 0,
		    "POST body size was %lld, larger than max of %d",
		    st.st_size, MPU_V2_COMMIT_POST_SIZE);
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
			mpu_v2_set_error(mpcr, NGX_HTTP_INTERNAL_SERVER_ERROR,
			    ngx_errno, "failed to pread %lld bytes from fd %d "
			    "at off %lld from post body", toread, fd, off);
			return (B_FALSE);
		}
		off += ret;
		st.st_size -= toread;

		if (ret == 0 && st.st_size != 0) {
			mpu_v2_set_error(mpcr, NGX_HTTP_INTERNAL_SERVER_ERROR, 0,
			    "hit EOF after reading %lld bytes, but still had "
			    "%lld remaining", off, st.st_size);
			return (B_FALSE);

		}
	} while (st.st_size > 0);

	ret = nvlist_parse_json(buf, off, nvl, NVJSON_FORCE_INTEGER, &nverr);

	if (ret != 0) {
		mpu_v2_set_error(mpcr, NGX_HTTP_BAD_REQUEST, nverr.nje_errno,
		    "mpu v2 commit encountered invalid JSON at pos %ld: %s",
		    nverr.nje_pos, nverr.nje_message);
		return (B_FALSE);
	}

	return (B_TRUE);
}

static boolean_t
mpu_v2_alloc_tmpfile(mpu_v2_request_t *mpcr, ngx_file_t *file)
{
	ngx_http_core_loc_conf_t *clcf;
	ngx_http_request_t *r = mpcr->mpcr_http;

	clcf = ngx_http_get_module_loc_conf(r, ngx_http_core_module);

	file->fd = NGX_INVALID_FILE;
	file->log = r->connection->log;

	if (ngx_create_temp_file(file, clcf->client_body_temp_path, r->pool,
	    1, 0, 0660) != NGX_OK) {
		mpu_v2_set_error(mpcr, NGX_HTTP_INTERNAL_SERVER_ERROR, 0,
		    "failed to create temporary file for MPU v2 output");
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
mpu_v2_verify_md5(mpu_v2_request_t *mpcr)
{
	if (mpcr->mpcr_req_md5 == NULL)
		return (B_TRUE);

	if (strcmp(mpcr->mpcr_req_md5, mpcr->mpcr_md5_b64) != 0) {
		/*
		 * We use a 469 which is what we use elsewhere in mako.
		 */
		mpu_v2_set_error(mpcr, 469, 0, "md5 checksums mismatched: client "
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
mpu_v2_convert_md5(mpu_v2_request_t *mpcr)
{
	ngx_str_t md5, b64;

	md5.data = (u_char *)mpcr->mpcr_md5_buf;
	md5.len = MD5_DIGEST_LENGTH;
	b64.data = (u_char *)mpcr->mpcr_md5_b64;
	ngx_encode_base64(&b64, &md5);
	if (b64.len != ngx_base64_encoded_length(MD5_DIGEST_LENGTH)) {
		mpu_v2_set_error(mpcr, NGX_HTTP_INTERNAL_SERVER_ERROR, 0,
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
mpu_v2_determine_output_md5(mpu_v2_request_t *mpcr, int fd)
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
			mpu_v2_set_error(mpcr, NGX_HTTP_INTERNAL_SERVER_ERROR,
			    errno, "failed to read from input file, fd %d", fd);
			return (B_FALSE);
		}

		ngx_md5_update(&mpcr->mpcr_md5, mpcr->mpcr_buf, ret);
	}

	ngx_md5_final(mpcr->mpcr_md5_buf, &mpcr->mpcr_md5);
	if (!mpu_v2_convert_md5(mpcr))
		return (B_FALSE);

	if (!mpu_v2_verify_md5(mpcr))
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
static mpu_v2_status_t
mpu_v2_check_exists(mpu_v2_request_t *mpcr)
{
	int fd, ret;
	char outfile[PATH_MAX];
	char outdir[PATH_MAX];
	struct stat sb;

	if ((ret = snprintf(outfile, sizeof (outfile), "%s/v2/%s/%s/%.2s/%s,%s",
	    mpcr->mpcr_root, mpcr->mpcr_owner, mpcr->mpcr_bucket_id,
	    mpcr->mpcr_object_id, mpcr->mpcr_object_id, mpcr->mpcr_object_hash)) >= PATH_MAX) {
		mpu_v2_set_error(mpcr, NGX_HTTP_INTERNAL_SERVER_ERROR, 0,
		    "failed to assemble mpu v2 output file, overflowed internal "
		    "snprintf buffer, needed %d bytes, had %d", ret,
		    sizeof (outfile));
		return (MPU_V2_FAILURE);
	}

	if ((fd = ngx_open_file(outfile, O_RDONLY | O_NOCTTY, NGX_FILE_OPEN,
	    0660)) < 0) {
		VERIFY3S(errno, !=, EFAULT);
		if (errno == ENOENT)
			return (MPU_V2_SUCCESS);
		mpu_v2_set_error(mpcr, NGX_HTTP_INTERNAL_SERVER_ERROR, ngx_errno,
		    "failed to open output file %s", outfile);
		return (MPU_V2_FAILURE);
	}

	if (ngx_fd_info(fd, &sb) != 0) {
		VERIFY3S(errno, !=, EFAULT);
		mpu_v2_set_error(mpcr, NGX_HTTP_INTERNAL_SERVER_ERROR, ngx_errno,
		    "failed to stat output file %s", outfile);

		(void) ngx_close_file(fd);
		return (MPU_V2_FAILURE);
	}

	if (sb.st_size != mpcr->mpcr_nbytes) {
		(void) ngx_close_file(fd);

		mpu_v2_set_error(mpcr, NGX_HTTP_CONFLICT, 0,
		    "size of MPU v2 output file %s is actually %lld, expected "
		    "%lld", outfile, sb.st_size, mpcr->mpcr_nbytes);
		return (MPU_V2_FAILURE);
	}

	ret = mpu_v2_determine_output_md5(mpcr, fd);
	(void) ngx_close_file(fd);
	if (ret == B_FALSE) {
		return (MPU_V2_FAILURE);
	}

	/*
	 * fsync(2) the parent directory.
	 */
	(void) snprintf(outdir, sizeof (outdir), "%s/v2/%s/%s/%.2s", 
	    mpcr->mpcr_root, mpcr->mpcr_owner, mpcr->mpcr_bucket_id,
	    mpcr->mpcr_object_id);
	fd = ngx_open_file(outdir, O_RDONLY | O_NOCTTY, NGX_FILE_OPEN, 0660);
	if (fd < 0) {
		VERIFY3S(errno, !=, EFAULT);
		mpu_v2_set_error(mpcr, NGX_HTTP_INTERNAL_SERVER_ERROR, errno,
		    "failed to open output directory %s for syncing", outdir);
		return (MPU_V2_FAILURE);
	}

	if (fsync(fd) != 0) {
		mpu_v2_set_error(mpcr, NGX_HTTP_INTERNAL_SERVER_ERROR, errno,
		    "failed to fsync parent directory %s", outdir);
		(void) ngx_close_file(fd);
		return (MPU_V2_FAILURE);
	}
	(void) ngx_close_file(fd);

	return (MPU_V2_EALREADY);
}

static boolean_t
mpu_v2_append_file(mpu_v2_request_t *mpcr, int fromfd, int tofd)
{
	for (;;) {
		ssize_t ret, towrite;
		off_t off;

		ret = read(fromfd, mpcr->mpcr_buf, mpcr->mpcr_buflen);
		if (ret == 0)
			break;
		if (ret < 0) {
			VERIFY3S(errno, !=, EFAULT);
			mpu_v2_set_error(mpcr, NGX_HTTP_INTERNAL_SERVER_ERROR,
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
				mpu_v2_set_error(mpcr,
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
 * Validate that a bucket-style part path is within expected boundaries
 * and doesn't contain directory traversal attempts.
 */
static boolean_t
mpu_v2_validate_part_path(const char *path)
{
	const char *p;
	size_t len;
	
	if (path == NULL)
		return (B_FALSE);
		
	len = strlen(path);
	if (len == 0 || len >= PATH_MAX)
		return (B_FALSE);
	
	/* Path must start with /manta/ */
	if (len < 7 || strncmp(path, "/manta/", 7) != 0)
		return (B_FALSE);
	
	/* Check for directory traversal attempts */
	p = path;
	while ((p = strstr(p, "..")) != NULL) {
		/* Check if it's actually ".." (not just substring) */
		if ((p == path || *(p - 1) == '/') &&
		    (*(p + 2) == '\0' || *(p + 2) == '/')) {
			return (B_FALSE);
		}
		p += 2;
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
mpu_v2_rename(mpu_v2_request_t *mpcr, ngx_file_t *infile)
{
	int ret, dirfd;
	char outfile[PATH_MAX];
	char outdir[PATH_MAX];
	ngx_http_request_t *r;

	r = mpcr->mpcr_http;
	if ((ret = snprintf(outfile, sizeof (outfile), "%s/v2/%s/%s/%.2s/%s,%s",
	    mpcr->mpcr_root, mpcr->mpcr_owner, mpcr->mpcr_bucket_id,
	    mpcr->mpcr_object_id, mpcr->mpcr_object_id, mpcr->mpcr_object_hash)) >= PATH_MAX) {
		mpu_v2_set_error(mpcr, NGX_HTTP_INTERNAL_SERVER_ERROR, 0,
		    "attempt to create path for output file with bucket "
		    "%s/%s and object %s,%s overflowed internal buffer, needed "
		    "%d bytes, had %d", mpcr->mpcr_owner, mpcr->mpcr_bucket_id,
		    mpcr->mpcr_object_id, mpcr->mpcr_object_hash, ret, PATH_MAX);
		if (ngx_delete_file(infile->name.data) == NGX_FILE_ERROR) {
			VERIFY3S(errno, !=, EFAULT);
			ngx_log_error(NGX_LOG_ERR, r->connection->log, errno,
			    "failed to remove temporary file %s after size "
			    "check failed", infile->name.data);
		}
		return (B_FALSE);
	}

	if (fsync(infile->fd) != 0) {
		mpu_v2_set_error(mpcr, NGX_HTTP_INTERNAL_SERVER_ERROR, errno,
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
		mpu_v2_set_error(mpcr, NGX_HTTP_INTERNAL_SERVER_ERROR, errno,
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

	/* Create a copy for dirname() since it may modify the string */
	(void) snprintf(outdir, sizeof (outdir), "%s", outfile);
	if ((dirfd = ngx_open_file(dirname(outdir), O_RDONLY|O_NOCTTY,
	    NGX_FILE_OPEN, 0660)) < 0) {
		VERIFY3S(errno, !=, EFAULT);
		mpu_v2_set_error(mpcr, NGX_HTTP_INTERNAL_SERVER_ERROR, errno,
		    "failed to open output directory %s for syncing", outdir);
		return (B_FALSE);
	}

	if (fsync(dirfd) != 0) {
		mpu_v2_set_error(mpcr, NGX_HTTP_INTERNAL_SERVER_ERROR, errno,
		    "failed to fsync parent directory %s", outdir);
		(void) ngx_close_file(dirfd);
		return (B_FALSE);
	}
	(void) ngx_close_file(dirfd);

	return (B_TRUE);
}

/*
 * Go through and make sure that every part path is valid and that it fits inside of
 * our internal buffers. For v2, we validate the full bucket-style paths.
 */
static boolean_t
mpu_v2_check_parts(mpu_v2_request_t *mpcr, mpu_v2_part_t *parts, ngx_uint_t nparts)
{
	ngx_uint_t i;

	for (i = 0; i < nparts; i++) {
		mpu_v2_part_t *part = &parts[i];

		if (!mpu_v2_validate_part_path(part->path)) {
			mpu_v2_set_error(mpcr, NGX_HTTP_BAD_REQUEST, 0,
			    "part %d has invalid path \"%s\"", 
			    part->part_number, part->path ? part->path : "(null)");
			return (B_FALSE);
		}

		if (part->size <= 0) {
			mpu_v2_set_error(mpcr, NGX_HTTP_BAD_REQUEST, 0,
			    "part %d has invalid size %lld", 
			    part->part_number, part->size);
			return (B_FALSE);
		}

		/* Basic ETag validation - should be quoted hex string */
		if (part->etag == NULL || strlen(part->etag) < 3 ||
		    part->etag[0] != '"' || part->etag[strlen(part->etag) - 1] != '"') {
			mpu_v2_set_error(mpcr, NGX_HTTP_BAD_REQUEST, 0,
			    "part %d has invalid ETag format \"%s\"", 
			    part->part_number, part->etag ? part->etag : "(null)");
			return (B_FALSE);
		}
	}

	return (B_TRUE);
}

static mpu_v2_status_t
mpu_v2_append_parts(mpu_v2_request_t *mpcr, mpu_v2_part_t *parts, 
    ngx_uint_t nparts, ngx_file_t *tmpfile)
{
	ngx_uint_t i;

	for (i = 0; i < nparts; i++) {
		mpu_v2_part_t *part = &parts[i];
		int fd;
		boolean_t ret;
		ssize_t bytes_read = 0;

		fd = ngx_open_file(part->path, O_RDONLY|O_NOCTTY, NGX_FILE_OPEN, 0660);
		if (fd < 0) {
			int err = errno;
			VERIFY3S(errno, !=, EFAULT);
			/*
			 * We encountered a missing part. If we're missing a
			 * part, then that might indicate that we're racing with
			 * another commit. As such, we try to check if the
			 * output file exists and if so, leverage that.
			 */
			if (errno == ENOENT && mpu_v2_check_exists(mpcr) ==
			    MPU_V2_EALREADY) {
				return (MPU_V2_EALREADY);
			}

			/*
			 * Clobber whatever error we may have encountered above
			 * with the actual thing we originally saw.
			 */
			mpu_v2_set_error(mpcr, NGX_HTTP_BAD_REQUEST, err,
			    "failed to open file %s for part %d", part->path, 
			    part->part_number);
			return (MPU_V2_FAILURE);
		}

		ret = mpu_v2_append_file(mpcr, fd, tmpfile->fd);
		(void) ngx_close_file(fd);
		if (ret == B_FALSE)
			return (MPU_V2_FAILURE);

		/* TODO: Optionally validate part ETag by calculating MD5 */
	}

	return (MPU_V2_SUCCESS);
}

static void
mpu_v2_cleanup_parts(mpu_v2_request_t *mpcr, mpu_v2_part_t *parts, ngx_uint_t nparts)
{
	ngx_uint_t i;
	ngx_http_request_t *r = mpcr->mpcr_http;

	for (i = 0; i < nparts; i++) {
		mpu_v2_part_t *part = &parts[i];

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
		if (unlink(part->path) != 0 && errno != ENOENT) {
			VERIFY3S(errno, !=, EFAULT);
			ngx_log_error(NGX_LOG_ERR, r->connection->log, errno,
			    "failed to remove file %s for part %d", part->path, 
			    part->part_number);
		}
	}
}

static boolean_t
mpu_v2_verify_size(mpu_v2_request_t *mpcr, ngx_file_t *fp)
{
	struct stat st;

	if (ngx_fd_info(fp->fd, &st) != 0) {
		mpu_v2_set_error(mpcr, NGX_HTTP_INTERNAL_SERVER_ERROR, ngx_errno,
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
		mpu_v2_set_error(mpcr, NGX_HTTP_CONFLICT, 0,
		    "assembled temporary file %s has size %lld bytes, request "
		    "specified %lld bytes", fp->name.data, st.st_size,
		    mpcr->mpcr_nbytes);
		return (B_FALSE);
	}

	return (B_TRUE);
}

/*
 * Parse the v2 JSON format and extract part information.
 */
static boolean_t
mpu_v2_parse_parts(mpu_v2_request_t *mpcr, nvlist_t *parts_nvl, 
    mpu_v2_part_t **parts_out, ngx_uint_t *nparts_out)
{
	ngx_uint_t nparts, i;
	mpu_v2_part_t *parts;
	ngx_http_request_t *r = mpcr->mpcr_http;

	if (nvlist_lookup_boolean(parts_nvl, ".__json_array") != 0 ||
	    nvlist_lookup_uint32(parts_nvl, "length", &nparts) != 0) {
		mpu_v2_set_error(mpcr, NGX_HTTP_BAD_REQUEST, 0,
		    "mpu v2 commit JSON parts not parsed to a valid array");
		return (B_FALSE);
	}

	if (nparts == 0 || nparts > MPU_V2_MAX_PARTS) {
		mpu_v2_set_error(mpcr, NGX_HTTP_BAD_REQUEST, 0,
		    "mpu v2 commit has invalid number of parts: %d", nparts);
		return (B_FALSE);
	}

	parts = ngx_pcalloc(r->pool, nparts * sizeof (mpu_v2_part_t));
	if (parts == NULL) {
		mpu_v2_set_error(mpcr, NGX_HTTP_INTERNAL_SERVER_ERROR, 0,
		    "failed to allocate memory for %d parts", nparts);
		return (B_FALSE);
	}

	for (i = 0; i < nparts; i++) {
		char key[64];
		nvlist_t *part_nvl;
		int64_t part_number, size;
		char *path, *etag;

		(void) snprintf(key, sizeof (key), "%d", i);
		if (nvlist_lookup_nvlist(parts_nvl, key, &part_nvl) != 0) {
			mpu_v2_set_error(mpcr, NGX_HTTP_BAD_REQUEST, 0,
			    "part %d is not a valid object", i);
			return (B_FALSE);
		}

		/* Extract part number */
		if (nvlist_lookup_int64(part_nvl, "partNumber", &part_number) != 0) {
			mpu_v2_set_error(mpcr, NGX_HTTP_BAD_REQUEST, 0,
			    "part %d missing partNumber", i);
			return (B_FALSE);
		}

		/* Extract part path */
		if (nvlist_lookup_string(part_nvl, "path", &path) != 0) {
			mpu_v2_set_error(mpcr, NGX_HTTP_BAD_REQUEST, 0,
			    "part %d missing path", i);
			return (B_FALSE);
		}

		/* Extract ETag */
		if (nvlist_lookup_string(part_nvl, "etag", &etag) != 0) {
			mpu_v2_set_error(mpcr, NGX_HTTP_BAD_REQUEST, 0,
			    "part %d missing etag", i);
			return (B_FALSE);
		}

		/* Extract size */
		if (nvlist_lookup_int64(part_nvl, "size", &size) != 0) {
			mpu_v2_set_error(mpcr, NGX_HTTP_BAD_REQUEST, 0,
			    "part %d missing size", i);
			return (B_FALSE);
		}

		parts[i].part_number = (ngx_uint_t)part_number;
		parts[i].path = ngx_pstrdup(r->pool, path);
		parts[i].etag = ngx_pstrdup(r->pool, etag);
		parts[i].size = size;

		if (parts[i].path == NULL || parts[i].etag == NULL) {
			mpu_v2_set_error(mpcr, NGX_HTTP_INTERNAL_SERVER_ERROR, 0,
			    "failed to allocate memory for part %d strings", i);
			return (B_FALSE);
		}
	}

	*parts_out = parts;
	*nparts_out = nparts;
	return (B_TRUE);
}

/*
 * Validate the various parts of the MPU v2 commit message and process them.
 */
static boolean_t
mpu_v2_nvl_process(mpu_v2_request_t *mpcr, nvlist_t *nvl)
{
	int ret;
	ngx_uint_t nparts;
	int64_t version, nbytes;
	char *owner, *bucket_id, *object_id, *object_hash, *upload_id, *req_md5;
	nvlist_t *parts_nvl;
	mpu_v2_part_t *parts;
	ngx_file_t tmpfile;
	ngx_http_request_t *r;

	r = mpcr->mpcr_http;
	if ((ret = nvlist_lookup_int64(nvl, "version", &version)) != 0) {
		mpu_v2_set_error(mpcr, NGX_HTTP_BAD_REQUEST, ret,
		    "mpu v2 commit JSON missing version");
		return (B_FALSE);
	}

	if (version != 2) {
		mpu_v2_set_error(mpcr, NGX_HTTP_BAD_REQUEST, 0,
		    "mpu v2 commit encountered unknown version: %lld", version);
		return (B_FALSE);
	}

	if ((ret = nvlist_lookup_int64(nvl, "nbytes", &nbytes)) != 0) {
		mpu_v2_set_error(mpcr, NGX_HTTP_BAD_REQUEST, ret,
		    "mpu v2 commit JSON missing nbytes");
		return (B_FALSE);
	}

	if (nbytes <= 0) {
		mpu_v2_set_error(mpcr, NGX_HTTP_BAD_REQUEST, 0,
		    "mpu v2 commit encountered invalid bytes: %lld", nbytes);
		return (B_FALSE);
	}

	if ((ret = nvlist_lookup_string(nvl, "owner", &owner)) != 0) {
		mpu_v2_set_error(mpcr, NGX_HTTP_BAD_REQUEST, ret,
		    "mpu v2 commit JSON missing owner");
		return (B_FALSE);
	}

	if ((ret = nvlist_lookup_string(nvl, "bucketId", &bucket_id)) != 0) {
		mpu_v2_set_error(mpcr, NGX_HTTP_BAD_REQUEST, ret,
		    "mpu v2 commit JSON missing bucketId");
		return (B_FALSE);
	}

	if ((ret = nvlist_lookup_string(nvl, "objectId", &object_id)) != 0) {
		mpu_v2_set_error(mpcr, NGX_HTTP_BAD_REQUEST, ret,
		    "mpu v2 commit JSON missing objectId");
		return (B_FALSE);
	}

	if ((ret = nvlist_lookup_string(nvl, "objectHash", &object_hash)) != 0) {
		mpu_v2_set_error(mpcr, NGX_HTTP_BAD_REQUEST, ret,
		    "mpu v2 commit JSON missing objectHash");
		return (B_FALSE);
	}

	if ((ret = nvlist_lookup_string(nvl, "uploadId", &upload_id)) != 0) {
		mpu_v2_set_error(mpcr, NGX_HTTP_BAD_REQUEST, ret,
		    "mpu v2 commit JSON missing uploadId");
		return (B_FALSE);
	}

	if ((ret = nvlist_lookup_nvlist(nvl, "parts", &parts_nvl)) != 0) {
		mpu_v2_set_error(mpcr, NGX_HTTP_BAD_REQUEST, ret,
		    "mpu v2 commit JSON missing parts");
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
			mpu_v2_set_error(mpcr, NGX_HTTP_BAD_REQUEST, 0,
			    "client md5 value is not properly b64 encoded, "
			    "expected %d bytes, got %d",
			    ngx_base64_encoded_length(16), ret);
			return (B_FALSE);
		}

		dec.data = buf;
		enc.data = (u_char *)req_md5;
		enc.len = ngx_base64_encoded_length(16);
		if (ngx_decode_base64(&dec, &enc) != NGX_OK) {
			mpu_v2_set_error(mpcr, NGX_HTTP_BAD_REQUEST, 0,
			    "client md5 value is not properly b64 encoded, "
			    "failed to decode b64 value");
			return (B_FALSE);
		}

		mpcr->mpcr_req_md5 = req_md5;
	}

	/* Basic validation for path traversal */
	if (strchr(owner, '.') != NULL || strchr(owner, '/') != NULL) {
		mpu_v2_set_error(mpcr, NGX_HTTP_BAD_REQUEST, 0,
		    "found illegal character, '.' or '/' in owner name");
		return (B_FALSE);
	}

	if (strchr(bucket_id, '.') != NULL || strchr(bucket_id, '/') != NULL) {
		mpu_v2_set_error(mpcr, NGX_HTTP_BAD_REQUEST, 0,
		    "found illegal character, '.' or '/' in bucketId name");
		return (B_FALSE);
	}

	if (strchr(object_id, '.') != NULL || strchr(object_id, '/') != NULL) {
		mpu_v2_set_error(mpcr, NGX_HTTP_BAD_REQUEST, 0,
		    "found illegal character, '.' or '/' in objectId name");
		return (B_FALSE);
	}

	if (strchr(object_hash, '.') != NULL || strchr(object_hash, '/') != NULL) {
		mpu_v2_set_error(mpcr, NGX_HTTP_BAD_REQUEST, 0,
		    "found illegal character, '.' or '/' in objectHash name");
		return (B_FALSE);
	}

	if (!mpu_v2_parse_parts(mpcr, parts_nvl, &parts, &nparts))
		return (B_FALSE);

	if (!mpu_v2_check_parts(mpcr, parts, nparts))
		return (B_FALSE);

	mpcr->mpcr_owner = owner;
	mpcr->mpcr_bucket_id = bucket_id;
	mpcr->mpcr_object_id = object_id;
	mpcr->mpcr_object_hash = object_hash;
	mpcr->mpcr_upload_id = upload_id;
	mpcr->mpcr_nbytes = nbytes;
	mpcr->mpcr_nparts = nparts;
	mpcr->mpcr_parts = parts;

	ret = mpu_v2_check_exists(mpcr);
	if (ret == MPU_V2_FAILURE)
		return (B_FALSE);
	if (ret == MPU_V2_EALREADY)
		goto cleanup;

	/*
	 * We'll validate the parts on the fly.
	 */
	bzero(&tmpfile, sizeof (tmpfile));
	if (!mpu_v2_alloc_tmpfile(mpcr, &tmpfile))
		return (B_FALSE);

	if ((ret = mpu_v2_append_parts(mpcr, parts, nparts, &tmpfile)) !=
	    MPU_V2_SUCCESS) {
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

		if (ret == MPU_V2_EALREADY)
			goto cleanup;
		return (B_FALSE);
	}

	if (!mpu_v2_verify_size(mpcr, &tmpfile)) {
		if (ngx_delete_file(tmpfile.name.data) == NGX_FILE_ERROR) {
			VERIFY3S(errno, !=, EFAULT);
			ngx_log_error(NGX_LOG_ERR, r->connection->log, errno,
			    "failed to remove temporary file %s after size "
			    "check failed", tmpfile.name.data);
		}
		return (B_FALSE);
	}

	ngx_md5_final(mpcr->mpcr_md5_buf, &mpcr->mpcr_md5);

	if (!mpu_v2_convert_md5(mpcr) || !mpu_v2_verify_md5(mpcr)) {
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
	if (!mpu_v2_rename(mpcr, &tmpfile))
		return (B_FALSE);

cleanup:
	mpu_v2_cleanup_parts(mpcr, parts, nparts);

	return (B_TRUE);
}

/*
 * This is called in the context of a given MPU v2 request. Here we must do the
 * heavy lifting of parsing the request and taking the actual actions.
 */
static void
mpu_v2_task_handler(void *arg, ngx_log_t *log)
{
	nvlist_t *nvl;
	mpu_v2_request_t *mpcr = arg;
	char *buf;

	buf = ngx_alloc(MPU_V2_COMMIT_RW_SIZE, mpcr->mpcr_http->connection->log);
	if (buf == NULL) {
		mpcr->mpcr_status = NGX_HTTP_INTERNAL_SERVER_ERROR;
		return;
	}

	mpcr->mpcr_buf = buf;
	mpcr->mpcr_buflen = MPU_V2_COMMIT_RW_SIZE;
	if (!mpu_v2_readin_request(mpcr, &nvl)) {
		ngx_free(buf);
		mpcr->mpcr_buf = NULL;
		mpcr->mpcr_buflen = 0;
		return;
	}

	if (mpu_v2_nvl_process(mpcr, nvl)) {
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
mpu_v2_post_thread(ngx_event_t *ev)
{
	int ret;
	mpu_v2_request_t *mpcr = ev->data;
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

		ngx_str_set(&r->headers_out.content_type, mpu_v2_content);
		r->headers_out.content_type.len = strlen(mpu_v2_content);
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
mpu_v2_post_body(ngx_http_request_t *r)
{
	ngx_thread_task_t *task;
	mpu_v2_request_t *mpcr;
	mpu_v2_loc_conf_t *conf;

	if (r->request_body == NULL) {
		ngx_log_error(NGX_LOG_CRIT, r->connection->log, 0, "mpu v2 body "
		    "callback missing request body!");
		ngx_http_finalize_request(r, NGX_HTTP_INTERNAL_SERVER_ERROR);
		return;
	}

	if (r->request_body->temp_file == NULL) {
		ngx_log_error(NGX_LOG_CRIT, r->connection->log, 0, "mpu v2 body "
		    "callback missing request temporary file!");
		ngx_http_finalize_request(r, NGX_HTTP_INTERNAL_SERVER_ERROR);
		return;
	}

	conf = ngx_http_get_module_loc_conf(r, ngx_http_mpu_v2_commit_module);
	task = ngx_thread_task_alloc(r->pool, sizeof (mpu_v2_request_t));
	if (task == NULL) {
		atomic_inc_64(&mpu_v2_overloads);
		ngx_log_error(NGX_LOG_CRIT, r->connection->log, 0,
		    "failed to allcoate MPU v2 request structure");
		ngx_http_finalize_request(r, NGX_HTTP_INTERNAL_SERVER_ERROR);
		return;
	}

	mpcr = task->ctx;
	mpcr->mpcr_http = r;
	mpcr->mpcr_task = task;
	mpcr->mpcr_root = (const char *)conf->mcl_root.data;
	mpcr->mpcr_status = NGX_HTTP_INTERNAL_SERVER_ERROR;
	ngx_md5_init(&mpcr->mpcr_md5);

	task->handler = mpu_v2_task_handler;
	task->event.data = mpcr;
	task->event.handler = mpu_v2_post_thread;

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
mpu_v2_handler(ngx_http_request_t *r)
{
	mpu_v2_loc_conf_t *conf;

	conf = ngx_http_get_module_loc_conf(r, ngx_http_mpu_v2_commit_module);
	if (conf->mcl_enabled != 1) {
		return (NGX_DECLINED);
	}

	if (r->method != NGX_HTTP_POST) {
		ngx_log_error(NGX_LOG_ERR, r->connection->log, 0,
		    "Only \"POST\" requests allowed for MPU v2");
		return (NGX_HTTP_NOT_ALLOWED);
	}

	r->request_body_in_file_only = 1;
	r->request_body_in_persistent_file = 1;
	r->request_body_in_clean_file = 1;
	r->request_body_file_group_access = 1;
	r->request_body_file_log_level = 0;

	return (ngx_http_read_client_request_body(r, mpu_v2_post_body));
}

static ngx_int_t
mpu_v2_init(ngx_conf_t *cf)
{
	ngx_http_handler_pt	*h;
	ngx_http_core_main_conf_t  *cmcf;

	cmcf = ngx_http_conf_get_module_main_conf(cf, ngx_http_core_module);

	h = ngx_array_push(&cmcf->phases[NGX_HTTP_CONTENT_PHASE].handlers);
	if (h == NULL) {
		return (NGX_ERROR);
	}

	*h = mpu_v2_handler;

	return (NGX_OK);
}

static ngx_http_module_t ngx_http_mpu_v2_commit_module_ctx = {
	NULL,			/* preconfiguration */
	mpu_v2_init,		/* postconfiguration */

	NULL,			/* create main configuration */
	NULL,			/* init main configuration */

	NULL,			/* create server configuration */
	NULL,			/* merge server configuration */

	mpu_v2_create_loc_conf,	/* create location configuration */
	mpu_v2_merge_loc_conf	/* merge location configuration */
};

ngx_module_t ngx_http_mpu_v2_commit_module = {
	NGX_MODULE_V1,
	&ngx_http_mpu_v2_commit_module_ctx,  /* module context */
	mpu_v2_commands,		     /* module directives */
	NGX_HTTP_MODULE,		     /* module type */
	NULL,				     /* init master */
	NULL,				     /* init module */
	NULL,				     /* init process */
	NULL,				     /* init thread */
	NULL,				     /* exit thread */
	NULL,				     /* exit process */
	NULL,				     /* exit master */
	NGX_MODULE_V1_PADDING
};