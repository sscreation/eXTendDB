
/*
    Copyright (C) 2013 Sridhar Valaguru <sridharnitt@gmail.com>

    This file is part of eXTendDB.

    eXTendDB is free software: you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation, either version 3 of the License, or
    (at your option) any later version.

    eXTendDB is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.

    You should have received a copy of the GNU General Public License
    along with eXTendDB.  If not, see <http://www.gnu.org/licenses/>.
*/

#ifndef __XTDB_H__
#define __XTDB_H__

#include <stdio.h>
#include <stdlib.h>
//#include <sys/statfs.h>
#include <sys/stat.h>
#include <unistd.h>
#include <dirent.h>
#include <string.h>
#include <stdint.h>
#include <errno.h>
#include <stddef.h>
#include <stdarg.h>

#include "query.h"
#include "bson.h"
#include "stringutil.h"
#include "dbbe.h"
#include "extenddberror.h"

struct XTDBHandle;
typedef struct XTDBHandle* XTDBHandlePtr;
/*!
 * Initializes a handle for XTDB to be used in all operations.
 * 
 * @param dbName -- Database name to be opened/created.
 * @param dataDir-- Directory to store the database files.
 * @param out    -- Output parameter holds the pointer to XTDBHandle
 */
XTDBError XTDBInitHandle(const char* dbName,const char* dataDir,XTDBHandlePtr* out);
/**
 * Frees XTDBHandle.
 * @param handle -- Handle to XTDB .
 */
void XTDBFreeHandle(XTDBHandlePtr handle);

/**
 * Creates index of a field.
 * 
 * @param handle    -- Handle to XTDB .
 * @param fieldName -- Name of the field to be indexed.
 * \returns True if successful or error is set.
 * \retval error is set to XTDB_INDEX_EXISTS if index exists.
 */
uint8_t XTDBCreateIndex(XTDBHandlePtr handle,char* fieldName);

/**
 * Removes an index.
 * 
 * @param handle    -- Handle to XTDB .
 * @param fieldName -- Name of the field to be indexed.
 * \returns True if successful or error is set.
 */
uint8_t XTDBDropIndex(XTDBHandlePtr handle,char* fieldName);

/**
 * Removes entire XTDB database and files.
 * 
 * @param handle    -- Handle to XTDB .
 * \returns True if successful or error is set.
 */

XTDBError XTDBDrop(XTDBHandlePtr handle);
/**
 * Syncs cached data to files/disk.
 * 
 * @param handle    -- Handle to XTDB .
 * \param diskSync  -- if True all data is synced to disk.
                       if False all data cached in memory is synced to file.
 * \returns True if successful or error is set.
 */
uint8_t XTDBSync(XTDBHandlePtr handle,uint8_t diskSync);

typedef enum XTDBCursorIndexStatus {
   NO_INDEX=0,
   MAIN_INDEX=1,
   OTHER_INDEX=2,
}XTDBCursorIndexStatus;

typedef struct XTDBCursor {
   void* dbIter;
   BinaryStr query;
   XTDBHandlePtr mdbHandle;
   uint8_t usingIndex;
   int idx;
   uint8_t first;
   BinaryStr lastKey;
   uint8_t nextKeyStatus;
   uint64_t gen;
   DataBaseBE* curDB;
   XTDBError error;
}XTDBCursor;

/**
 * Inserts a bson object into the db.
 * 
 * @param handle  -- Handle to XTDB .
 * \param newVal  -- Complete bson object.
 * \returns True if successful or error is set.
 */
uint8_t XTDBInsert(XTDBHandlePtr handle,bson* newVal);

/**
 * Finds objects matching query bson object and returns cursor.
 * 
 * @param handle -- Handle to XTDB .
 * \param query  -- Bson query object
 * \returns cursor object.
 */
XTDBCursor* XTDBFind(XTDBHandlePtr handle,bson* query);
/**
 * Finds objects matching query bson object and returns cursor.
 * 
 * @param cursor      -- Cursor object returned by XTDBFind.
 * \param[out] outKey -- Returns _id of the next BSON object.
 * \param[out] outVal -- Returns next BSON object in the cursor.
 * \returns True if successful or error is set.
 */
uint8_t XTDBCursorNext(XTDBCursor* cursor,
                      BinaryStr* outKey,
                      bson* outVal);

/**
 * Frees cursor returned by XTDBFind.
 * 
 * @param cursor      -- Cursor object returned by XTDBFind.
 */
void XTDBCursorFree(XTDBCursor* cursor);

/**
 * Updates all bson objects matched by query.
 * 
 * @param handle -- Handle to XTDB .
 * \param query  -- Query bson object.
 * \param newVal -- Value of the updated bson object.
 * \param upsert -- If True if the query returns no object new object is returned.
 * \returns True if successful or error is set.
 */
uint8_t XTDBUpdate(XTDBHandlePtr handle,bson* query,bson* newVal,uint8_t upsert);

/**
 * Removes all bson objects matched by query.
 * 
 * @param handle -- Handle to XTDB .
 * \param query  -- Query bson object.
 * \returns True if successful or error is set.
 */
uint8_t XTDBRemove(XTDBHandlePtr handle,bson* query);
/**
 * Counts the number of objects matching the query.
 * 
 * @param handle -- Handle to XTDB .
 * \param query  -- Query bson object.
 * \returns Number of documents matching the query.
 */
int64_t XTDBCount(XTDBHandlePtr handle, bson* query);

/**
 * Counts the number of objects matching the query.
 * 
 * @param handle -- Handle to XTDB .
 * \returns Last occured error.
 */
XTDBError XTDBGetLastError(XTDBHandlePtr handle);
/**
 * Counts the number of objects matching the query.
 * 
 * @param handle -- Handle to XTDB .
 * \param[out] error -- Output bson object to hold error information.
 */
void XTDBGetLastErrorBson(XTDBHandlePtr handle,bson* error);

#endif
