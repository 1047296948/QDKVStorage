#import "QDKVStorage.h"
#import <UIKit/UIKit.h>

#if __has_include(<sqlite3.h>)
#import <sqlite3.h>
#else
#import "sqlite3.h"
#endif

static const NSUInteger kQDMaxErrorRetryCount = 8;
static const NSTimeInterval kQDMinRetryTimeInterval = 2.0;
static const int kQDPathLengthMax = PATH_MAX - 64;
static NSString *const kQDDBFileName = @"kvstorage.sqlite";
static NSString *const kQDDBShmFileName = @"kvstorage.sqlite-shm";
static NSString *const kQDDBWalFileName = @"kvstorage.sqlite-wal";

/*
 File:
   /path/
     /kvstorage.sqlite
     /kvstorage.sqlite-shm
     /kvstorage.sqlite-wal
 
 SQL:
   create table if not exists kvstorage (
     key                 text,
     data                blob,
     size                integer,
     modification_time   integer,
     primary key(key)
   );
   create index if not exists modification_time_idx on kvstorage(modification_time);
 */

/// Returns nil in App Extension.
static UIApplication *_QDSharedApplication() {
    static BOOL isAppExtension = NO;
    static dispatch_once_t onceToken;
    dispatch_once(&onceToken, ^{
        Class cls = NSClassFromString(@"UIApplication");
        if(!cls || ![cls respondsToSelector:@selector(sharedApplication)]) isAppExtension = YES;
        if ([[[NSBundle mainBundle] bundlePath] hasSuffix:@".appex"]) isAppExtension = YES;
    });
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wundeclared-selector"
    return isAppExtension ? nil : [UIApplication performSelector:@selector(sharedApplication)];
#pragma clang diagnostic pop
}

#if KVS_ENABLE_THREAD_SAFE
#   define Lock() dispatch_semaphore_wait(self->_lock, DISPATCH_TIME_FOREVER)
#   define Unlock() dispatch_semaphore_signal(self->_lock)
#else
#   define Lock() ;
#   define Unlock() ;
#endif

@implementation QDKVStorage {
    NSString *_path;
    NSString *_dbPath;

    sqlite3 *_db;
    CFMutableDictionaryRef _dbStmtCache;
    NSTimeInterval _dbLastOpenErrorTime;
    NSUInteger _dbOpenErrorCount;
    
    dispatch_semaphore_t _lock;
    
    BOOL _errorLogsEnabled;
}

#pragma mark - db

- (BOOL)_dbOpen {
    if (_db) return YES;
    
    int result = sqlite3_open(_dbPath.UTF8String, &_db);
    if (result == SQLITE_OK) {
        CFDictionaryKeyCallBacks keyCallbacks = kCFCopyStringDictionaryKeyCallBacks;
        CFDictionaryValueCallBacks valueCallbacks = {0};
        _dbStmtCache = CFDictionaryCreateMutable(CFAllocatorGetDefault(), 0, &keyCallbacks, &valueCallbacks);
        _dbLastOpenErrorTime = 0;
        _dbOpenErrorCount = 0;
        return YES;
    } else {
        _db = NULL;
        if (_dbStmtCache) CFRelease(_dbStmtCache);
        _dbStmtCache = NULL;
        _dbLastOpenErrorTime = CACurrentMediaTime();
        _dbOpenErrorCount++;
        
        if (_errorLogsEnabled) {
            NSLog(@"%s line:%d sqlite open failed (%d).", __FUNCTION__, __LINE__, result);
        }
        return NO;
    }
}

- (BOOL)_dbClose {
    if (!_db) return YES;
    
    int  result = 0;
    BOOL retry = NO;
    BOOL stmtFinalized = NO;
    
    if (_dbStmtCache) CFRelease(_dbStmtCache);
    _dbStmtCache = NULL;
    
    do {
        retry = NO;
        result = sqlite3_close(_db);
        if (result == SQLITE_BUSY || result == SQLITE_LOCKED) {
            if (!stmtFinalized) {
                stmtFinalized = YES;
                sqlite3_stmt *stmt;
                while ((stmt = sqlite3_next_stmt(_db, nil)) != 0) {
                    sqlite3_finalize(stmt);
                    retry = YES;
                }
            }
        } else if (result != SQLITE_OK) {
            if (_errorLogsEnabled) {
                NSLog(@"%s line:%d sqlite close failed (%d).", __FUNCTION__, __LINE__, result);
            }
        }
    } while (retry);
    _db = NULL;
    return YES;
}

- (BOOL)_dbCheck {
    if (!_db) {
        if (_dbOpenErrorCount < kQDMaxErrorRetryCount &&
            CACurrentMediaTime() - _dbLastOpenErrorTime > kQDMinRetryTimeInterval) {
            return [self _dbOpen] && [self _dbInitialize];
        } else {
            return NO;
        }
    }
    return YES;
}

- (BOOL)_dbInitialize {
    NSString *sql = @"pragma journal_mode = wal; pragma synchronous = normal; create table if not exists kvstorage (key text, data blob, size integer, modification_time integer, primary key(key));create index if not exists modification_time_idx on kvstorage(modification_time);";
    return [self _dbExecute:sql];
}

- (void)_dbCheckpoint {
    if (![self _dbCheck]) return;
    // Cause a checkpoint to occur, merge `sqlite-wal` file to `sqlite` file.
    sqlite3_wal_checkpoint(_db, NULL);
}

- (BOOL)_dbExecute:(NSString *)sql {
    if (sql.length == 0) return NO;
    if (![self _dbCheck]) return NO;
    
    char *error = NULL;
    int result = sqlite3_exec(_db, sql.UTF8String, NULL, NULL, &error);
    if (error) {
        if (_errorLogsEnabled) NSLog(@"%s line:%d sqlite exec error (%d): %s", __FUNCTION__, __LINE__, result, error);
        sqlite3_free(error);
    }
    
    return result == SQLITE_OK;
}

- (sqlite3_stmt *)_dbPrepareStmt:(NSString *)sql {
    if (![self _dbCheck] || sql.length == 0 || !_dbStmtCache) return NULL;
    sqlite3_stmt *stmt = (sqlite3_stmt *)CFDictionaryGetValue(_dbStmtCache, (__bridge const void *)(sql));
    if (!stmt) {
        int result = sqlite3_prepare_v2(_db, sql.UTF8String, -1, &stmt, NULL);
        if (result != SQLITE_OK) {
            if (_errorLogsEnabled) NSLog(@"%s line:%d sqlite stmt prepare error (%d): %s", __FUNCTION__, __LINE__, result, sqlite3_errmsg(_db));
            return NULL;
        }
        CFDictionarySetValue(_dbStmtCache, (__bridge const void *)(sql), stmt);
    } else {
        sqlite3_reset(stmt);
    }
    return stmt;
}

- (NSString *)_dbJoinedKeys:(NSArray *)keys {
    NSMutableString *string = [NSMutableString new];
    for (NSUInteger i = 0,max = keys.count; i < max; i++) {
        [string appendString:@"?"];
        if (i + 1 != max) {
            [string appendString:@","];
        }
    }
    return string;
}

- (void)_dbBindJoinedKeys:(NSArray *)keys stmt:(sqlite3_stmt *)stmt fromIndex:(int)index{
    for (int i = 0, max = (int)keys.count; i < max; i++) {
        NSString *key = keys[i];
        sqlite3_bind_text(stmt, index + i, key.UTF8String, -1, NULL);
    }
}

- (BOOL)_dbSaveWithKey:(NSString *)key data:(NSData *)data {
    NSString *sql = @"insert or replace into kvstorage (key, data, size, modification_time) values (?1, ?2, ?3, ?4);";
    sqlite3_stmt *stmt = [self _dbPrepareStmt:sql];
    if (!stmt) return NO;
    
    int timestamp = (int)time(NULL);
    sqlite3_bind_text(stmt, 1, key.UTF8String, -1, NULL);
    sqlite3_bind_blob(stmt, 2, data.bytes, (int)data.length, 0);
    sqlite3_bind_int(stmt, 3, (int)data.length);
    sqlite3_bind_int(stmt, 4, timestamp);
    
    int result = sqlite3_step(stmt);
    if (result != SQLITE_DONE) {
        if (_errorLogsEnabled) NSLog(@"%s line:%d sqlite insert error (%d): %s", __FUNCTION__, __LINE__, result, sqlite3_errmsg(_db));
        return NO;
    }
    return YES;
}

- (BOOL)_dbDeleteDataWithKey:(NSString *)key {
    NSString *sql = @"delete from kvstorage where key = ?1;";
    sqlite3_stmt *stmt = [self _dbPrepareStmt:sql];
    if (!stmt) return NO;
    sqlite3_bind_text(stmt, 1, key.UTF8String, -1, NULL);
    
    int result = sqlite3_step(stmt);
    if (result != SQLITE_DONE) {
        if (_errorLogsEnabled) NSLog(@"%s line:%d db delete error (%d): %s", __FUNCTION__, __LINE__, result, sqlite3_errmsg(_db));
        return NO;
    }
    return YES;
}

- (BOOL)_dbDeleteDataWithKeys:(NSArray *)keys {
    if (![self _dbCheck]) return NO;
    NSString *sql =  [NSString stringWithFormat:@"delete from kvstorage where key in (%@);", [self _dbJoinedKeys:keys]];
    sqlite3_stmt *stmt = NULL;
    int result = sqlite3_prepare_v2(_db, sql.UTF8String, -1, &stmt, NULL);
    if (result != SQLITE_OK) {
        if (_errorLogsEnabled) NSLog(@"%s line:%d sqlite stmt prepare error (%d): %s", __FUNCTION__, __LINE__, result, sqlite3_errmsg(_db));
        return NO;
    }
    
    [self _dbBindJoinedKeys:keys stmt:stmt fromIndex:1];
    result = sqlite3_step(stmt);
    sqlite3_finalize(stmt);
    if (result == SQLITE_ERROR) {
        if (_errorLogsEnabled) NSLog(@"%s line:%d sqlite delete error (%d): %s", __FUNCTION__, __LINE__, result, sqlite3_errmsg(_db));
        return NO;
    }
    return YES;
}

- (NSData*)_dbGetDataFromStmt:(sqlite3_stmt *)stmt {
    int i = 0;
    const void *data = sqlite3_column_blob(stmt, i);
    int data_bytes = sqlite3_column_bytes(stmt, i++);
    
    if (data_bytes > 0 && data) return [NSData dataWithBytes:data length:data_bytes];
    return nil;
}

- (NSData*)_dbGetDataWithKey:(NSString *)key {
    NSString *sql = @"select data from kvstorage where key = ?1;";
    sqlite3_stmt *stmt = [self _dbPrepareStmt:sql];
    if (!stmt) return nil;
    sqlite3_bind_text(stmt, 1, key.UTF8String, -1, NULL);
    
    int result = sqlite3_step(stmt);
    if (result == SQLITE_ROW) {
        return [self _dbGetDataFromStmt:stmt];
    } else {
        if (result != SQLITE_DONE) {
            if (_errorLogsEnabled) NSLog(@"%s line:%d sqlite query error (%d): %s", __FUNCTION__, __LINE__, result, sqlite3_errmsg(_db));
        }
    }
    return nil;
}

- (NSMutableArray *)_dbGetDataWithKeys:(NSArray *)keys {
    if (![self _dbCheck]) return nil;
    NSString *sql = [NSString stringWithFormat:@"select data from kvstorage where key in (%@) order by modification_time desc", [self _dbJoinedKeys:keys]];
    
    sqlite3_stmt *stmt = NULL;
    int result = sqlite3_prepare_v2(_db, sql.UTF8String, -1, &stmt, NULL);
    if (result != SQLITE_OK) {
        if (_errorLogsEnabled) NSLog(@"%s line:%d sqlite stmt prepare error (%d): %s", __FUNCTION__, __LINE__, result, sqlite3_errmsg(_db));
        return nil;
    }
    
    [self _dbBindJoinedKeys:keys stmt:stmt fromIndex:1];
    NSMutableArray *datas = [NSMutableArray new];
    do {
        result = sqlite3_step(stmt);
        if (result == SQLITE_ROW) {
            NSData *data = [self _dbGetDataFromStmt:stmt];
            if (data) [datas addObject:data];
        } else if (result == SQLITE_DONE) {
            break;
        } else {
            if (_errorLogsEnabled) NSLog(@"%s line:%d sqlite query error (%d): %s", __FUNCTION__, __LINE__, result, sqlite3_errmsg(_db));
            datas = nil;
            break;
        }
    } while (1);
    sqlite3_finalize(stmt);
    return datas;
}

- (NSMutableArray *)_dbGetAllData {
    if (![self _dbCheck]) return nil;
    NSString *sql = @"select data from kvstorage order by modification_time desc";
    sqlite3_stmt *stmt = [self _dbPrepareStmt:sql];
    if (!stmt) return nil;
    int result = sqlite3_step(stmt);
    if (result != SQLITE_OK) {
        if (_errorLogsEnabled) NSLog(@"%s line:%d sqlite stmt prepare error (%d): %s", __FUNCTION__, __LINE__, result, sqlite3_errmsg(_db));
        return nil;
    }
    
    NSMutableArray *datas = [NSMutableArray new];
    do {
        result = sqlite3_step(stmt);
        if (result == SQLITE_ROW) {
            NSData *data = [self _dbGetDataFromStmt:stmt];
            if (data) [datas addObject:data];
        } else if (result == SQLITE_DONE) {
            break;
        } else {
            if (_errorLogsEnabled) NSLog(@"%s line:%d sqlite query error (%d): %s", __FUNCTION__, __LINE__, result, sqlite3_errmsg(_db));
            datas = nil;
            break;
        }
    } while (1);
    sqlite3_finalize(stmt);
    return datas;
}

- (int)_dbGetAllDataCount {
    if (![self _dbCheck]) return -1;
    NSString *sql = @"select count(*) from kvstorage;";
    sqlite3_stmt *stmt = [self _dbPrepareStmt:sql];
    if (!stmt) return -1;
    int result = sqlite3_step(stmt);
    if (result != SQLITE_ROW) {
        if (_errorLogsEnabled) NSLog(@"%s line:%d sqlite query error (%d): %s", __FUNCTION__, __LINE__, result, sqlite3_errmsg(_db));
        return -1;
    }
    return sqlite3_column_int(stmt, 0);
}

- (int)_dbGetDataCountWithKey:(NSString *)key {
    if (![self _dbCheck]) return -1;
    NSString *sql = @"select count(key) from kvstorage where key = ?1;";
    sqlite3_stmt *stmt = [self _dbPrepareStmt:sql];
    if (!stmt) return -1;
    sqlite3_bind_text(stmt, 1, key.UTF8String, -1, NULL);
    int result = sqlite3_step(stmt);
    if (result != SQLITE_ROW) {
        if (_errorLogsEnabled) NSLog(@"%s line:%d sqlite query error (%d): %s", __FUNCTION__, __LINE__, result, sqlite3_errmsg(_db));
        return -1;
    }
    return sqlite3_column_int(stmt, 0);
}

- (BOOL)_dbDataExistsForKey:(NSString *)key {
    if (![self _dbCheck]) return NO;
    NSString *sql = @"select 1 from kvstorage where key = ?1;";
    sqlite3_stmt *stmt = [self _dbPrepareStmt:sql];
    if (!stmt) return NO;
    sqlite3_bind_text(stmt, 1, key.UTF8String, -1, NULL);
    int result = sqlite3_step(stmt);
    if (result == SQLITE_ROW) {
        return YES;
    } else {
        if (result != SQLITE_DONE) {
            if (_errorLogsEnabled) NSLog(@"%s line:%d sqlite query error (%d): %s", __FUNCTION__, __LINE__, result, sqlite3_errmsg(_db));
        }
    }
    return NO;
}

- (int)_dbGetTotalSize {
    NSString *sql = @"select sum(size) from kvstorage;";
    sqlite3_stmt *stmt = [self _dbPrepareStmt:sql];
    if (!stmt) return -1;
    int result = sqlite3_step(stmt);
    if (result != SQLITE_ROW) {
        if (_errorLogsEnabled) NSLog(@"%s line:%d sqlite query error (%d): %s", __FUNCTION__, __LINE__, result, sqlite3_errmsg(_db));
        return -1;
    }
    return sqlite3_column_int(stmt, 0);
}

#pragma mark - private

/**
 Delete all files and empty in background.
 Make sure the db is closed.
 */
- (void)_reset {
    [[NSFileManager defaultManager] removeItemAtPath:[_path stringByAppendingPathComponent:kQDDBFileName] error:nil];
    [[NSFileManager defaultManager] removeItemAtPath:[_path stringByAppendingPathComponent:kQDDBShmFileName] error:nil];
    [[NSFileManager defaultManager] removeItemAtPath:[_path stringByAppendingPathComponent:kQDDBWalFileName] error:nil];
}

#pragma mark - public

- (instancetype)init {
    @throw [NSException exceptionWithName:@"QDKVStorage init error" reason:@"Please use the designated initializer and pass the 'path'." userInfo:nil];
    return [self initWithPath:@""];
}

- (instancetype)initWithPath:(NSString *)path {
    if (path.length == 0 || path.length > kQDPathLengthMax) {
        NSLog(@"QDKVStorage init error: invalid path: [%@].", path);
        return nil;
    }
    
    self = [super init];
    _path = path.copy;
    _dbPath = [path stringByAppendingPathComponent:kQDDBFileName];
    _errorLogsEnabled = KVS_ENABLE_ERROR_LOGS ? YES : NO;
    NSError *error = nil;
    if (![[NSFileManager defaultManager] createDirectoryAtPath:path
                                   withIntermediateDirectories:YES
                                                    attributes:nil
                                                         error:&error]) {
            NSLog(@"QDKVStorage init error:%@", error);
            return nil;
        }
    
    if (![self _dbOpen] || ![self _dbInitialize]) {
        // db file may broken...
        [self _dbClose];
        [self _reset]; // rebuild
        if (![self _dbOpen] || ![self _dbInitialize]) {
            [self _dbClose];
            NSLog(@"QDKVStorage init error: fail to open sqlite db.");
            return nil;
        }
    }
    _lock = dispatch_semaphore_create(1);
    return self;
}

- (void)dealloc {
    UIBackgroundTaskIdentifier taskID = [_QDSharedApplication() beginBackgroundTaskWithExpirationHandler:^{}];
    [self _dbClose];
    if (taskID != UIBackgroundTaskInvalid) {
        [_QDSharedApplication() endBackgroundTask:taskID];
    }
}

- (BOOL)setValue:(nullable id<NSCoding>)value forKey:(nullable NSString*)key {
    if (!key.length || !value) return NO;
    NSData* data = nil;
    @try {
        data = [NSKeyedArchiver archivedDataWithRootObject:value];
    }
    @catch (NSException *exception) {
        // nothing to do...
    }
    if (!data) return NO;
    Lock();
    BOOL ret = [self _dbSaveWithKey:key data:data];
    Unlock();
    return ret;
}

- (BOOL)removeValueForKey:(nullable NSString*)key {
    if (!key.length) return NO;
    Lock();
    BOOL ret = [self _dbDeleteDataWithKey:key];
    Unlock();
    return ret;
}

- (BOOL)removeValuesForKeys:(nullable NSArray<NSString*>*)keys {
    if (!keys.count) return NO;
    Lock();
    BOOL ret = [self _dbDeleteDataWithKeys:keys];
    Unlock();
    return ret;
}

- (BOOL)removeAllValues {
    Lock();
    BOOL ret = [self _dbClose];
    if (ret) {
        [self _reset];
        ret = [self _dbOpen];
        if (ret) {
            ret = [self _dbInitialize];
        }
    }
    Unlock();
    return ret;
}

- (nullable id<NSCoding>)valueForKey:(nullable NSString*)key {
    if (!key.length) return nil;
    Lock();
    NSData* data = [self _dbGetDataWithKey:key];
    Unlock();
    if (!data) return nil;
    @try {
        return [NSKeyedUnarchiver unarchiveObjectWithData:data];
    }
    @catch (NSException *exception) {
        // nothing to do...
    }
    return nil;
}

- (BOOL)valueExistsForKey:(nullable NSString*)key {
    if (!key.length) return NO;
    Lock();
    BOOL ret = [self _dbDataExistsForKey:key];
    Unlock();
    return ret;
}

- (nullable NSArray<id<NSCoding>>*)getAllValues {
    Lock();
    NSArray* datas = [self _dbGetAllData];
    Unlock();
    if (!datas) return nil;
    @try {
        NSMutableArray* ret = [NSMutableArray arrayWithCapacity:datas.count];
        for (NSData* data in datas) {
            id<NSCoding> object = [NSKeyedUnarchiver unarchiveObjectWithData:data];
            if (!object) return nil;
            [ret addObject:object];
        }
        return ret;
    }
    @catch (NSException *exception) {
        // nothing to do...
    }
    return nil;
}

- (NSInteger)getAllValuesCount {
    Lock();
    int ret = [self _dbGetAllDataCount];
    Unlock();
    return (NSInteger)ret;
}

- (NSInteger)getValuesTotalSize {
    Lock();
    long long ret = [self _dbGetTotalSize];
    Unlock();
    return (NSInteger)ret;
}

@end
