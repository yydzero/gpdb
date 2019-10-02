/*
 * src/port/pthread-win32.h
 */
#ifndef __PTHREAD_H
#define __PTHREAD_H

#define PTHREAD_STACK_MIN 8192

typedef struct win32_pthread* pthread_t;
typedef int pthread_attr_t;

typedef ULONG pthread_key_t;
typedef CRITICAL_SECTION *pthread_mutex_t;
typedef int pthread_once_t;

/* Maybe better to put signal related code to separated file. */
typedef int sigset_t;

DWORD		pthread_self(void);

int			pthread_attr_init(pthread_attr_t* attr);
int			pthread_attr_destroy(pthread_attr_t* attr);

void		pthread_setspecific(pthread_key_t, void *);
void	   *pthread_getspecific(pthread_key_t);

int			pthread_mutex_init(pthread_mutex_t *, void *attr);
int			pthread_mutex_lock(pthread_mutex_t *);

/* blocking */
int			pthread_mutex_unlock(pthread_mutex_t *);

#endif
