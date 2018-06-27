/*-------------------------------------------------------------------------
 *
 * be-secure.c
 *	  functions related to setting up a secure connection to the frontend.
 *	  Secure connections are expected to provide confidentiality,
 *	  message integrity and endpoint authentication.
 *
 *
 * Portions Copyright (c) 1996-2015, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/libpq/be-secure.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include <sys/stat.h>
#include <signal.h>
#include <fcntl.h>
#include <ctype.h>
#include <sys/socket.h>
#include <unistd.h>
#include <netdb.h>
#include <netinet/in.h>
#ifdef HAVE_NETINET_TCP_H
#include <netinet/tcp.h>
#include <arpa/inet.h>
#endif

<<<<<<< HEAD
#include "miscadmin.h"

#ifdef USE_SSL
#include <openssl/ssl.h>
#include <openssl/dh.h>
#if SSLEAY_VERSION_NUMBER >= 0x0907000L
#include <openssl/conf.h>
#endif
#endif   /* USE_SSL */

=======
>>>>>>> ab93f90cd3a4fcdd891cee9478941c3cc65795b8
#include "libpq/libpq.h"
#include "miscadmin.h"
#include "tcop/tcopprot.h"
#include "utils/memutils.h"
#include "storage/proc.h"

#define ERROR_BUF_SIZE 32

char	   *ssl_cert_file;
char	   *ssl_key_file;
char	   *ssl_ca_file;
char	   *ssl_crl_file;

/*
 *	How much data can be sent across a secure connection
 *	(total in both directions) before we require renegotiation.
 *	Set to 0 to disable renegotiation completely.
 */
int			ssl_renegotiation_limit;

#ifdef USE_SSL
bool		ssl_loaded_verify_locations = false;
#endif

/* GUC variable controlling SSL cipher list */
char	   *SSLCipherSuites = NULL;

/* GUC variable for default ECHD curve. */
char	   *SSLECDHCurve;

/* GUC variable: if false, prefer client ciphers */
bool		SSLPreferServerCiphers;

/* ------------------------------------------------------------ */
/*			 Procedures common to all secure sessions			*/
/* ------------------------------------------------------------ */

/*
 *	Initialize global context
 */
int
secure_initialize(void)
{
#ifdef USE_SSL
	be_tls_init();
#endif

	return 0;
}

/*
 * Indicate if we have loaded the root CA store to verify certificates
 */
bool
secure_loaded_verify_locations(void)
{
#ifdef USE_SSL
	return ssl_loaded_verify_locations;
#else
	return false;
#endif
}

/*
 *	Attempt to negotiate secure session.
 */
int
secure_open_server(Port *port)
{
	int			r = 0;

#ifdef USE_SSL
	r = be_tls_open_server(port);
#endif

	return r;
}

/*
 *	Close secure session.
 */
void
secure_close(Port *port)
{
#ifdef USE_SSL
	if (port->ssl_in_use)
		be_tls_close(port);
#endif
}

/*
 *	Read data from a secure connection.
 */
ssize_t
secure_read(Port *port, void *ptr, size_t len)
{
	ssize_t		n;
	int			waitfor;

retry:
#ifdef USE_SSL
	waitfor = 0;
	if (port->ssl_in_use)
	{
		n = be_tls_read(port, ptr, len, &waitfor);
	}
	else
#endif
	{
		n = secure_raw_read(port, ptr, len);
		waitfor = WL_SOCKET_READABLE;
	}

	/* In blocking mode, wait until the socket is ready */
	if (n < 0 && !port->noblock && (errno == EWOULDBLOCK || errno == EAGAIN))
	{
		int			w;

		Assert(waitfor);

		w = WaitLatchOrSocket(MyLatch,
							  WL_LATCH_SET | waitfor,
							  port->sock, 0);

		/* Handle interrupt. */
		if (w & WL_LATCH_SET)
		{
			ResetLatch(MyLatch);
			ProcessClientReadInterrupt(true);

			/*
			 * We'll retry the read. Most likely it will return immediately
			 * because there's still no data available, and we'll wait for the
			 * socket to become ready again.
			 */
		}
		goto retry;
	}

	/*
	 * Process interrupts that happened while (or before) receiving. Note that
	 * we signal that we're not blocking, which will prevent some types of
	 * interrupts from being processed.
	 */
	ProcessClientReadInterrupt(false);

	return n;
}

ssize_t
secure_raw_read(Port *port, void *ptr, size_t len)
{
	ssize_t		n;

	/*
	 * Try to read from the socket without blocking. If it succeeds we're
	 * done, otherwise we'll wait for the socket using the latch mechanism.
	 */
#ifdef WIN32
	pgwin32_noblock = true;
#endif
	n = recv(port->sock, ptr, len, 0);
#ifdef WIN32
	pgwin32_noblock = false;
#endif

	return n;
}


/*
 * Report a COMMERROR.
 *
 * This function holds an interrupt before reporting this error to avoid
 * a self deadlock situation, see MPP-13718 for more info.
 */
#ifdef USE_SSL
static void
report_commerror(const char *err_msg)
{
	HOLD_INTERRUPTS();

	ereport(COMMERROR,
			(errcode(ERRCODE_PROTOCOL_VIOLATION),
			 errmsg("%s",err_msg)));

	RESUME_INTERRUPTS();
}
#endif

/*
 *	Write data to a secure connection.
 */
ssize_t
secure_write(Port *port, void *ptr, size_t len)
{
	ssize_t		n;
	int			waitfor;

retry:
	waitfor = 0;
#ifdef USE_SSL
	if (port->ssl_in_use)
	{
<<<<<<< HEAD
		int			err;

		if (ssl_renegotiation_limit && port->count > ssl_renegotiation_limit * 1024L)
		{
			SSL_set_session_id_context(port->ssl, (void *) &SSL_context,
									   sizeof(SSL_context));
			if (SSL_renegotiate(port->ssl) <= 0)
			{
				report_commerror("SSL renegotiation failure");
			}

			if (SSL_do_handshake(port->ssl) <= 0)
			{
				report_commerror("SSL renegotiation failure");
			}

			if (port->ssl->state != SSL_ST_OK)
			{
				report_commerror("SSL failed to send renegotiation request");
			}

			port->ssl->state |= SSL_ST_ACCEPT;
			SSL_do_handshake(port->ssl);
			if (port->ssl->state != SSL_ST_OK)
			{
				report_commerror("SSL renegotiation failure");
			}

			port->count = 0;
		}

wloop:
		errno = 0;
		n = SSL_write(port->ssl, ptr, len);
		err = SSL_get_error(port->ssl, n);

		const int ERR_MSG_LEN = ERROR_BUF_SIZE + 20;
		char err_msg[ERR_MSG_LEN];

		switch (err)
		{
			case SSL_ERROR_NONE:
				port->count += n;
				break;
			case SSL_ERROR_WANT_READ:
			case SSL_ERROR_WANT_WRITE:
#ifdef WIN32
				pgwin32_waitforsinglesocket(SSL_get_fd(port->ssl),
											(err == SSL_ERROR_WANT_READ) ?
									FD_READ | FD_CLOSE : FD_WRITE | FD_CLOSE,
											INFINITE);
#endif
				goto wloop;
			case SSL_ERROR_SYSCALL:
				/* leave it to caller to ereport the value of errno */
				if (n != -1)
				{
					errno = ECONNRESET;
					n = -1;
				}
				break;
			case SSL_ERROR_SSL:
				snprintf((char *)&err_msg, ERR_MSG_LEN, "SSL error: %s", SSLerrmessage());
				report_commerror(err_msg);
				/* fall through */
			case SSL_ERROR_ZERO_RETURN:
				errno = ECONNRESET;
				n = -1;
				break;
			default:
				snprintf((char *)&err_msg, ERR_MSG_LEN, "unrecognized SSL error code: %d", err);
				report_commerror(err_msg);

				n = -1;
				break;
		}
	}
	else
#endif
	{
		prepare_for_client_write();
		n = send(port->sock, ptr, len, 0);
		client_write_ended();
	}

	return n;
}

/* ------------------------------------------------------------ */
/*						  SSL specific code						*/
/* ------------------------------------------------------------ */
#ifdef USE_SSL

/*
 * Private substitute BIO: this does the sending and receiving using send() and
 * recv() instead. This is so that we can enable and disable interrupts
 * just while calling recv(). We cannot have interrupts occurring while
 * the bulk of openssl runs, because it uses malloc() and possibly other
 * non-reentrant libc facilities. We also need to call send() and recv()
 * directly so it gets passed through the socket/signals layer on Win32.
 *
 * They are closely modelled on the original socket implementations in OpenSSL.
 */

static bool my_bio_initialized = false;
static BIO_METHOD my_bio_methods;

static int
my_sock_read(BIO *h, char *buf, int size)
{
	int			res = 0;

	prepare_for_client_read();

	if (buf != NULL)
	{
		res = recv(h->num, buf, size, 0);
		BIO_clear_retry_flags(h);
		if (res <= 0)
		{
			/* If we were interrupted, tell caller to retry */
			if (errno == EINTR)
			{
				BIO_set_retry_read(h);
			}
		}
	}

	client_read_ended();

	return res;
}

static int
my_sock_write(BIO *h, const char *buf, int size)
{
	int			res = 0;

	prepare_for_client_write();

	res = send(h->num, buf, size, 0);
	if (res <= 0)
	{
		if (errno == EINTR)
		{
			BIO_set_retry_write(h);
		}
	}

	client_write_ended();

	return res;
}

static BIO_METHOD *
my_BIO_s_socket(void)
{
	if (!my_bio_initialized)
	{
		memcpy(&my_bio_methods, BIO_s_socket(), sizeof(BIO_METHOD));
		my_bio_methods.bread = my_sock_read;
		my_bio_methods.bwrite = my_sock_write;
		my_bio_initialized = true;
	}
	return &my_bio_methods;
}

/* This should exactly match openssl's SSL_set_fd except for using my BIO */
static int
my_SSL_set_fd(SSL *s, int fd)
{
	int			ret = 0;
	BIO		   *bio = NULL;

	bio = BIO_new(my_BIO_s_socket());

	if (bio == NULL)
	{
		SSLerr(SSL_F_SSL_SET_FD, ERR_R_BUF_LIB);
		goto err;
	}
	BIO_set_fd(bio, fd, BIO_NOCLOSE);
	SSL_set_bio(s, bio, bio);
	ret = 1;
err:
	return ret;
}

/*
 *	Load precomputed DH parameters.
 *
 *	To prevent "downgrade" attacks, we perform a number of checks
 *	to verify that the DBA-generated DH parameters file contains
 *	what we expect it to contain.
 */
static DH  *
load_dh_file(int keylength)
{
	FILE	   *fp;
	char		fnbuf[MAXPGPATH];
	DH		   *dh = NULL;
	int			codes;

	/* attempt to open file.  It's not an error if it doesn't exist. */
	snprintf(fnbuf, sizeof(fnbuf), "dh%d.pem", keylength);
	if ((fp = fopen(fnbuf, "r")) == NULL)
		return NULL;

/*	flock(fileno(fp), LOCK_SH); */
	dh = PEM_read_DHparams(fp, NULL, NULL, NULL);
/*	flock(fileno(fp), LOCK_UN); */
	fclose(fp);

	/* is the prime the correct size? */
	if (dh != NULL && 8 * DH_size(dh) < keylength)
	{
		elog(LOG, "DH errors (%s): %d bits expected, %d bits found",
			 fnbuf, keylength, 8 * DH_size(dh));
		dh = NULL;
	}

	/* make sure the DH parameters are usable */
	if (dh != NULL)
	{
		if (DH_check(dh, &codes) == 0)
		{
			elog(LOG, "DH_check error (%s): %s", fnbuf, SSLerrmessage());
			return NULL;
		}
		if (codes & DH_CHECK_P_NOT_PRIME)
		{
			elog(LOG, "DH error (%s): p is not prime", fnbuf);
			return NULL;
		}
		if ((codes & DH_NOT_SUITABLE_GENERATOR) &&
			(codes & DH_CHECK_P_NOT_SAFE_PRIME))
		{
			elog(LOG,
				 "DH error (%s): neither suitable generator or safe prime",
				 fnbuf);
			return NULL;
		}
	}

	return dh;
}

/*
 *	Load hardcoded DH parameters.
 *
 *	To prevent problems if the DH parameters files don't even
 *	exist, we can load DH parameters hardcoded into this file.
 */
static DH  *
load_dh_buffer(const char *buffer, size_t len)
{
	BIO		   *bio;
	DH		   *dh = NULL;

	bio = BIO_new_mem_buf((char *) buffer, len);
	if (bio == NULL)
		return NULL;
	dh = PEM_read_bio_DHparams(bio, NULL, NULL, NULL);
	if (dh == NULL)
		ereport(DEBUG2,
				(errmsg_internal("DH load buffer: %s",
								 SSLerrmessage())));
	BIO_free(bio);

	return dh;
}

/*
 *	Generate an ephemeral DH key.  Because this can take a long
 *	time to compute, we can use precomputed parameters of the
 *	common key sizes.
 *
 *	Since few sites will bother to precompute these parameter
 *	files, we also provide a fallback to the parameters provided
 *	by the OpenSSL project.
 *
 *	These values can be static (once loaded or computed) since
 *	the OpenSSL library can efficiently generate random keys from
 *	the information provided.
 */
static DH  *
tmp_dh_cb(SSL *s, int is_export, int keylength)
{
	DH		   *r = NULL;
	static DH  *dh = NULL;
	static DH  *dh512 = NULL;
	static DH  *dh1024 = NULL;
	static DH  *dh2048 = NULL;
	static DH  *dh4096 = NULL;

	switch (keylength)
	{
		case 512:
			if (dh512 == NULL)
				dh512 = load_dh_file(keylength);
			if (dh512 == NULL)
				dh512 = load_dh_buffer(file_dh512, sizeof file_dh512);
			r = dh512;
			break;

		case 1024:
			if (dh1024 == NULL)
				dh1024 = load_dh_file(keylength);
			if (dh1024 == NULL)
				dh1024 = load_dh_buffer(file_dh1024, sizeof file_dh1024);
			r = dh1024;
			break;

		case 2048:
			if (dh2048 == NULL)
				dh2048 = load_dh_file(keylength);
			if (dh2048 == NULL)
				dh2048 = load_dh_buffer(file_dh2048, sizeof file_dh2048);
			r = dh2048;
			break;

		case 4096:
			if (dh4096 == NULL)
				dh4096 = load_dh_file(keylength);
			if (dh4096 == NULL)
				dh4096 = load_dh_buffer(file_dh4096, sizeof file_dh4096);
			r = dh4096;
			break;

		default:
			if (dh == NULL)
				dh = load_dh_file(keylength);
			r = dh;
	}

	/* this may take a long time, but it may be necessary... */
	if (r == NULL || 8 * DH_size(r) < keylength)
	{
		ereport(DEBUG2,
				(errmsg_internal("DH: generating parameters (%d bits)",
								 keylength)));
		r = DH_generate_parameters(keylength, DH_GENERATOR_2, NULL, NULL);
	}

	return r;
}

/*
 *	Certificate verification callback
 *
 *	This callback allows us to log intermediate problems during
 *	verification, but for now we'll see if the final error message
 *	contains enough information.
 *
 *	This callback also allows us to override the default acceptance
 *	criteria (e.g., accepting self-signed or expired certs), but
 *	for now we accept the default checks.
 */
static int
verify_cb(int ok, X509_STORE_CTX *ctx)
{
	return ok;
}

/*
 *	This callback is used to copy SSL information messages
 *	into the PostgreSQL log.
 */
static void
info_cb(const SSL *ssl, int type, int args)
{
	switch (type)
=======
		n = be_tls_write(port, ptr, len, &waitfor);
	}
	else
#endif
>>>>>>> ab93f90cd3a4fcdd891cee9478941c3cc65795b8
	{
		n = secure_raw_write(port, ptr, len);
		waitfor = WL_SOCKET_WRITEABLE;
	}

	if (n < 0 && !port->noblock && (errno == EWOULDBLOCK || errno == EAGAIN))
	{
<<<<<<< HEAD
#if SSLEAY_VERSION_NUMBER >= 0x0907000L
		OPENSSL_config(NULL);
#endif
		SSL_library_init();
		SSL_load_error_strings();

		/*
		 * We use SSLv23_method() because it can negotiate use of the highest
		 * mutually supported protocol version, while alternatives like
		 * TLSv1_2_method() permit only one specific version.  Note that we
		 * don't actually allow SSL v2, only v3 and TLS protocols (see below).
		 */
		SSL_context = SSL_CTX_new(SSLv23_method());
		if (!SSL_context)
			ereport(FATAL,
					(errmsg("could not create SSL context: %s",
							SSLerrmessage())));

		/*
		 * Disable OpenSSL's moving-write-buffer sanity check, because it
		 * causes unnecessary failures in nonblocking send cases.
		 */
		SSL_CTX_set_mode(SSL_context, SSL_MODE_ACCEPT_MOVING_WRITE_BUFFER);
=======
		int			w;
>>>>>>> ab93f90cd3a4fcdd891cee9478941c3cc65795b8

		Assert(waitfor);

		w = WaitLatchOrSocket(MyLatch,
							  WL_LATCH_SET | waitfor,
							  port->sock, 0);

		/* Handle interrupt. */
		if (w & WL_LATCH_SET)
		{
<<<<<<< HEAD
			/* Set the flags to check against the complete CRL chain */
			if (X509_STORE_load_locations(cvstore, ssl_crl_file, NULL) == 1)
			{
				/* OpenSSL 0.96 does not support X509_V_FLAG_CRL_CHECK */
#ifdef X509_V_FLAG_CRL_CHECK
				X509_STORE_set_flags(cvstore,
						  X509_V_FLAG_CRL_CHECK | X509_V_FLAG_CRL_CHECK_ALL);
#else
				ereport(LOG,
				(errmsg("SSL certificate revocation list file \"%s\" ignored",
						ssl_crl_file),
				 errdetail("SSL library does not support certificate revocation lists.")));
#endif
			}
			else
				ereport(FATAL,
						(errmsg("could not load SSL certificate revocation list file \"%s\": %s",
								ssl_crl_file, SSLerrmessage())));
		}
	}

	if (ssl_ca_file[0])
	{
		/*
		 * Always ask for SSL client cert, but don't fail if it's not
		 * presented.  We might fail such connections later, depending on what
		 * we find in pg_hba.conf.
		 */
		SSL_CTX_set_verify(SSL_context,
						   (SSL_VERIFY_PEER |
							SSL_VERIFY_CLIENT_ONCE),
						   verify_cb);

		/* Set flag to remember CA store is successfully loaded */
		ssl_loaded_verify_locations = true;

		/*
		 * Tell OpenSSL to send the list of root certs we trust to clients in
		 * CertificateRequests.  This lets a client with a keystore select the
		 * appropriate client certificate to send to us.
		 */
		SSL_CTX_set_client_CA_list(SSL_context, root_cert_list);
	}
}

/*
 *	Attempt to negotiate SSL connection.
 */
static int
open_server_SSL(Port *port)
{
	int			r;
	int			err;

	Assert(!port->ssl);
	Assert(!port->peer);

	if (!(port->ssl = SSL_new(SSL_context)))
	{
		ereport(COMMERROR,
				(errcode(ERRCODE_PROTOCOL_VIOLATION),
				 errmsg("could not initialize SSL connection: %s",
						SSLerrmessage())));
		return -1;
	}
	if (!my_SSL_set_fd(port->ssl, port->sock))
	{
		ereport(COMMERROR,
				(errcode(ERRCODE_PROTOCOL_VIOLATION),
				 errmsg("could not set SSL socket: %s",
						SSLerrmessage())));
		return -1;
	}

aloop:
	r = SSL_accept(port->ssl);
	if (r <= 0)
	{
		err = SSL_get_error(port->ssl, r);
		switch (err)
		{
			case SSL_ERROR_WANT_READ:
			case SSL_ERROR_WANT_WRITE:
#ifdef WIN32
				pgwin32_waitforsinglesocket(SSL_get_fd(port->ssl),
											(err == SSL_ERROR_WANT_READ) ?
						FD_READ | FD_CLOSE | FD_ACCEPT : FD_WRITE | FD_CLOSE,
											INFINITE);
#endif
				goto aloop;
			case SSL_ERROR_SYSCALL:
				if (r < 0)
					ereport(COMMERROR,
							(errcode_for_socket_access(),
							 errmsg("could not accept SSL connection: %m")));
				else
					ereport(COMMERROR,
							(errcode(ERRCODE_PROTOCOL_VIOLATION),
					errmsg("could not accept SSL connection: EOF detected")));
				break;
			case SSL_ERROR_SSL:
				ereport(COMMERROR,
						(errcode(ERRCODE_PROTOCOL_VIOLATION),
						 errmsg("could not accept SSL connection: %s",
								SSLerrmessage())));
				break;
			case SSL_ERROR_ZERO_RETURN:
				ereport(COMMERROR,
						(errcode(ERRCODE_PROTOCOL_VIOLATION),
				   errmsg("could not accept SSL connection: EOF detected")));
				break;
			default:
				ereport(COMMERROR,
						(errcode(ERRCODE_PROTOCOL_VIOLATION),
						 errmsg("unrecognized SSL error code: %d",
								err)));
				break;
		}
		return -1;
	}

	port->count = 0;

	/* Get client certificate, if available. */
	port->peer = SSL_get_peer_certificate(port->ssl);

	/* and extract the Common Name from it. */
	port->peer_cn = NULL;
	if (port->peer != NULL)
	{
		int			len;

		len = X509_NAME_get_text_by_NID(X509_get_subject_name(port->peer),
										NID_commonName, NULL, 0);
		if (len != -1)
		{
			char	   *peer_cn;

			peer_cn = MemoryContextAlloc(TopMemoryContext, len + 1);
			r = X509_NAME_get_text_by_NID(X509_get_subject_name(port->peer),
										  NID_commonName, peer_cn, len + 1);
			peer_cn[len] = '\0';
			if (r != len)
			{
				/* shouldn't happen */
				pfree(peer_cn);
				close_SSL(port);
				return -1;
			}
=======
			ResetLatch(MyLatch);
			ProcessClientWriteInterrupt(true);
>>>>>>> ab93f90cd3a4fcdd891cee9478941c3cc65795b8

			/*
			 * We'll retry the write. Most likely it will return immediately
			 * because there's still no data available, and we'll wait for the
			 * socket to become ready again.
			 */
		}
		goto retry;
	}

	/*
	 * Process interrupts that happened while (or before) sending. Note that
	 * we signal that we're not blocking, which will prevent some types of
	 * interrupts from being processed.
	 */
	ProcessClientWriteInterrupt(false);

	return n;
}

ssize_t
secure_raw_write(Port *port, const void *ptr, size_t len)
{
	ssize_t		n;

#ifdef WIN32
	pgwin32_noblock = true;
#endif
	n = send(port->sock, ptr, len, 0);
#ifdef WIN32
	pgwin32_noblock = false;
#endif

	return n;
}
<<<<<<< HEAD

/*
 * Obtain reason string for last SSL error
 *
 * Some caution is needed here since ERR_reason_error_string will
 * return NULL if it doesn't recognize the error code.  We don't
 * want to return NULL ever.
 */
static const char *
SSLerrmessage(void)
{
	unsigned long errcode;
	const char *errreason;
	static char errbuf[ERROR_BUF_SIZE];

	errcode = ERR_get_error();
	if (errcode == 0)
		return _("no SSL error reported");
	errreason = ERR_reason_error_string(errcode);
	if (errreason != NULL)
		return errreason;
	snprintf(errbuf, ERROR_BUF_SIZE, _("SSL error code %lu"), errcode);
	return errbuf;
}

#endif   /* USE_SSL */
=======
>>>>>>> ab93f90cd3a4fcdd891cee9478941c3cc65795b8
