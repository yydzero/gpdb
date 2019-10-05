/*-------------------------------------------------------------------------
*
* pthread-win32.c
*	 partial pthread implementation for win32
*
* Copyright (c) 2004-2014, PostgreSQL Global Development Group
* IDENTIFICATION
*	src/interfaces/libpq/pthread-win32.c
*
*-------------------------------------------------------------------------
*/

#include "postgres_fe.h"

#include <windows.h>
#include "pthread-win32.h"

DWORD
pthread_self(void)
{
	return GetCurrentThreadId();
}

int
pthread_attr_init(pthread_attr_t* attr)
{
}

int
pthread_attr_setstacksize(pthread_attr_t* attr, size_t stacksize)
{
}

int
pthread_attr_destroy(pthread_attr_t* attr)
{
}

void
pthread_setspecific(pthread_key_t key, void *val)
{
}

void *
pthread_getspecific(pthread_key_t key)
{
	return NULL;
}

int
pthread_mutex_init(pthread_mutex_t *mp, void *attr)
{
	*mp = (CRITICAL_SECTION *) malloc(sizeof(CRITICAL_SECTION));
	if (!*mp)
		return 1;
	InitializeCriticalSection(*mp);
	return 0;
}

int
pthread_mutex_lock(pthread_mutex_t *mp)
{
	if (!*mp)
		return 1;
	EnterCriticalSection(*mp);
	return 0;
}

int
pthread_mutex_trylock(pthread_mutex_t* mp)
{
	if (!*mp)
		return 1;
	TryEnterCriticalSection(*mp);
	return 0;
}

int
pthread_mutex_unlock(pthread_mutex_t *mp)
{
	if (!*mp)
		return 1;
	LeaveCriticalSection(*mp);
	return 0;
}

/* partial pthread implementation for Windows */

static unsigned __stdcall
win32_pthread_run(void* arg)
{
	win32_pthread* th = (win32_pthread*)arg;

	th->result = th->routine(th->arg);

	return 0;
}

int
pthread_create(pthread_t* thread,
	pthread_attr_t* attr,
	void* (*start_routine) (void*),
	void* arg)
{
	int			save_errno;
	win32_pthread* th;

	th = (win32_pthread*)malloc(sizeof(win32_pthread));
	if (th == NULL)
	{
		fprintf(stderr, "could not allocate memory for thread struct");
		exit(1);
	}
	th->routine = start_routine;
	th->arg = arg;
	th->result = NULL;

	// CreateThread()

	th->handle = (HANDLE)_beginthreadex(NULL, 0, win32_pthread_run, th, 0, NULL);
	if (th->handle == NULL)
	{
		save_errno = errno;
		free(th);
		return save_errno;
	}

	*thread = th;
	return 0;
}

int
pthread_join(pthread_t th, void** thread_return)
{
	if (th == NULL || th->handle == NULL)
		return errno = EINVAL;

	if (WaitForSingleObject(th->handle, INFINITE) != WAIT_OBJECT_0)
	{
		_dosmaperr(GetLastError());
		return errno;
	}

	if (thread_return)
		*thread_return = th->result;

	CloseHandle(th->handle);
	free(th);
	return 0;
}

int
pthread_equal(pthread_t t1, pthread_t t2)
{
	return (t1 == t2 || t1->handle == t2->handle);
}

int
pthread_kill(pthread_t thread, int sig)
{
	return 0;
}

void
pthread_exit(void* value_ptr)
{
	_endthread();
}