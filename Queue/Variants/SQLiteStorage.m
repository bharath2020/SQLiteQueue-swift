//  Copyright © 2017 Atlassian. All rights reserved.

#import "SQLiteStorage.h"
#import <sqlite3.h>

#define SQLITE_SAFE_FINALIZE(__ptr__) if( __ptr__ != NULL) { sqlite3_finalize(__ptr__); __ptr__ = NULL; }

static const char * INSERT_EVENTS = "INSERT INTO RECORDS values(?,?)";
static const char * CREATE_EVENTS_TABLE = "CREATE TABLE IF NOT EXISTS RECORDS(REC_ID TEXT UNIQUE, REC_DATA TEXT)";
static const char * SELECT_LIMITED_EVENTS = "SELECT * FROM RECORDS LIMIT ?";
static const char * TOTAL_EVENTS = "SELECT COUNT(*) FROM RECORDS";
static const char * BEGIN_TRANSACTION = "BEGIN TRANSACTION";
static const char * COMMIT_TRANSACTION = "COMMIT TRANSACTION";
static NSString * DELETE_EVENT_WITH_IDS = @"DELETE FROM RECORDS WHERE REC_ID IN ( %@ )";


@interface Record()
@property(nonatomic,strong, readwrite) NSString *identifier;
@property(nonatomic,strong, readwrite) NSString *payload;
@end

@implementation Record

- (instancetype)initWithIdentifier:(NSString *)identifier payload:(NSString *)payload {
    self = [super init];
    if(self) {
        if( [identifier isEqualToString:@""] || [payload isEqualToString:@""]) {
            return nil;
        }

        self.identifier = identifier;
        self.payload = payload;
    }
    return self;
}
@end


@interface SQLiteStorage() {
    sqlite3 *db;
    sqlite3_stmt *insertStatement;
    sqlite3_stmt *retrieveStatement;
    sqlite3_stmt *countStatment;
    sqlite3_stmt *beginTransaction;
    sqlite3_stmt *commitTransaction;
    dispatch_queue_t serialQueue;
}
@end

@implementation SQLiteStorage(DBSetup)
- (BOOL)createTable {
    NSAssert(db != NULL, @"Invoking create table without creating database connection");
    if ( db == NULL) {
        return NO;
    }

    int error = sqlite3_exec(db, CREATE_EVENTS_TABLE,  nil, nil, nil);
    return error == SQLITE_OK;
}

- (BOOL)prepareStatements {
    NSAssert(db != NULL, @"Invoking prepare statements without creating database connection");
    if (db == NULL) {
        return NO;
    }

    int error = sqlite3_prepare_v2(db, INSERT_EVENTS, -1, &insertStatement, nil);
    error = (error == SQLITE_OK) ? sqlite3_prepare_v2(db, SELECT_LIMITED_EVENTS, -1, &retrieveStatement, nil) : error;
    error = (error == SQLITE_OK) ? sqlite3_prepare_v2(db, TOTAL_EVENTS, -1, &countStatment, nil) : error;
    error = (error == SQLITE_OK) ? sqlite3_prepare_v2(db, BEGIN_TRANSACTION, -1, &beginTransaction, nil) : error;
    error = (error == SQLITE_OK) ? sqlite3_prepare_v2(db, COMMIT_TRANSACTION, -1, &commitTransaction, nil) : error;

    return (error == SQLITE_OK);
}

- (void)logSQLiteErrorMessage:(NSString *)message {
    NSLog(@"%@: %d - %s", message, sqlite3_errcode(db), sqlite3_errmsg(db));
}
@end

@implementation SQLiteStorage
- (instancetype _Nullable)initWithDabasePath:(NSString * _Nullable)path {
    self = [super init];
    if (self) {

        serialQueue = dispatch_queue_create("com.atlassian.atlassian-analytics.dbQueue", DISPATCH_QUEUE_SERIAL);
        NSAssert([path length] != 0, @"");
        BOOL dbExist = [[NSFileManager defaultManager] fileExistsAtPath:path];
        int error = sqlite3_open_v2([path cStringUsingEncoding:NSUTF8StringEncoding], &db, SQLITE_OPEN_CREATE | SQLITE_OPEN_READWRITE | SQLITE_OPEN_FULLMUTEX, nil);
        if( error != SQLITE_OK) {
            return nil;
        }

        if( !dbExist && ![self createTable] ) {
            [self logSQLiteErrorMessage:@"Failed to create table"];
            return nil;
        }

        if( ![self prepareStatements]) {
            [self logSQLiteErrorMessage:@"Failed to prepare statements"];
            return nil;
        }
    }
    return self;
}

- (void)dealloc {
    SQLITE_SAFE_FINALIZE(insertStatement)
    SQLITE_SAFE_FINALIZE(countStatment)
    SQLITE_SAFE_FINALIZE(retrieveStatement)
    SQLITE_SAFE_FINALIZE(beginTransaction)
    SQLITE_SAFE_FINALIZE(commitTransaction)

    if(db != NULL) {
        sqlite3_close_v2(db);
        db = NULL;
    }
}

- (void)addEvent:(Record * _Nonnull)record {
    dispatch_async(serialQueue, ^{
        BOOL success = sqlite3_bind_text(self->insertStatement,1, [record.identifier UTF8String], -1, SQLITE_STATIC) == SQLITE_OK;
        success &= (sqlite3_bind_text(self->insertStatement,2, [record.payload UTF8String], -1, SQLITE_STATIC) == SQLITE_OK);

        success &= (sqlite3_step(self->insertStatement) == SQLITE_DONE);

        sqlite3_clear_bindings(self->insertStatement);
        sqlite3_reset(self->insertStatement);
        if(!success) {
            [self logSQLiteErrorMessage:@"Failed to add record"];
        }
    });
}

- (void)removeEvents: (NSArray<NSString *> * _Nonnull)identifiers {
    if( [identifiers count] == 0 ){
        return;
    }

    dispatch_async(serialQueue, ^{
        __block NSMutableString *ids = [NSMutableString string];
        [identifiers enumerateObjectsUsingBlock:^(NSString * _Nonnull obj, NSUInteger idx, BOOL * _Nonnull stop) {
            if( idx != [identifiers count]-1 ) {
                [ids appendFormat:@"'%@', ", obj];
            }
            else {
                [ids appendFormat:@"'%@'", obj];
            }
        }];

        BOOL success = YES;
        NSString *query = [NSString stringWithFormat:DELETE_EVENT_WITH_IDS, ids];
        success = sqlite3_exec(db, [query UTF8String], nil, nil, nil) == SQLITE_OK;

        if (!success) {
            [self logSQLiteErrorMessage:@"Failed to remove records"];
        }
    });
}

- (void)nextEvents:(int)limit resultBlock: (void (^ _Nonnull)(NSArray<Record *> * _Nullable records))resultBlock {
    NSAssert(resultBlock != nil, @"Invoking nextEvents without resultBlock");
    if( resultBlock == nil ) {
        return;
    }

    if( limit < 1 ) {
        resultBlock(nil);
        return;
    }

    dispatch_async(serialQueue, ^{
        int error = sqlite3_bind_int(self->retrieveStatement, 1, limit);
        if (error != SQLITE_OK) {
            [self logSQLiteErrorMessage:@"Failed to bind insert statement"];
            resultBlock(nil);
            return;
        }

        NSMutableArray *records = [NSMutableArray array];
        while ((error = sqlite3_step(retrieveStatement)) == SQLITE_ROW) {
            const char * identifierCString = (const char*) sqlite3_column_text(retrieveStatement, 0);
            NSString *identifier = [NSString stringWithUTF8String:identifierCString];

            const char * payloadCString = (const char*) sqlite3_column_text(retrieveStatement, 1);
            NSString *payload = [NSString stringWithUTF8String:payloadCString];

            Record *record = [[Record alloc] initWithIdentifier:identifier payload:payload];
            if( record != nil) {
                [records addObject: record];
            }
        }

        if( error != SQLITE_DONE) {
            [self logSQLiteErrorMessage:@"Failed to fetch records"];
        }

        sqlite3_clear_bindings(self->retrieveStatement);
        sqlite3_reset(self->retrieveStatement);

        resultBlock( records );
    });
}

- (void)count: (void (^ _Nonnull)(NSInteger count))resultBlock {
    NSAssert(resultBlock != nil, @"Invoking count without resultBlock");
    if( resultBlock == nil ){
        return;
    }

    dispatch_async(serialQueue, ^{
        int error = sqlite3_step(self->countStatment);
        NSInteger count = 0;
        if( error == SQLITE_ROW) {
            count = sqlite3_column_int(self->countStatment, 0);
        }

        sqlite3_clear_bindings(self->countStatment);
        sqlite3_reset(self->countStatment);

        resultBlock(count);
    });
}

- (BOOL)beginTransaction {
    int error = sqlite3_step(beginTransaction);
    sqlite3_reset(beginTransaction);
    return  error == SQLITE_OK;
}

- (BOOL)commitTransaction {
    int error = sqlite3_step(commitTransaction);
    sqlite3_reset(beginTransaction);
    return  error == SQLITE_OK;
}

@end
