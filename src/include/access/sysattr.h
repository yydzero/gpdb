/*-------------------------------------------------------------------------
 *
 * sysattr.h
 *	  POSTGRES system attribute definitions.
 *
 *
 * Portions Copyright (c) 1996-2015, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/access/sysattr.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef SYSATTR_H
#define SYSATTR_H


/*
 * Attribute numbers for the system-defined attributes
 */
#define SelfItemPointerAttributeNumber			(-1)
#define ObjectIdAttributeNumber					(-2)
#define MinTransactionIdAttributeNumber			(-3)
#define MinCommandIdAttributeNumber				(-4)
#define MaxTransactionIdAttributeNumber			(-5)
#define MaxCommandIdAttributeNumber				(-6)
#define TableOidAttributeNumber					(-7)
<<<<<<< HEAD
#define GpSegmentIdAttributeNumber			    (-8)    /*CDB*/
#define FirstLowInvalidHeapAttributeNumber		(-9)
=======
#define FirstLowInvalidHeapAttributeNumber		(-8)
>>>>>>> ab93f90cd3a4fcdd891cee9478941c3cc65795b8

#endif   /* SYSATTR_H */
