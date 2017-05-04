//
//  SQLiteStorage.m
//  Queue
//
//  Created by Bharath Booshan on 5/3/17.
//  Copyright © 2017 Atlassian. All rights reserved.
//

#import "SQLiteStorage.h"
#import <sqlite3.h>

#define SQLITE_SAFE_FINALIZE(__ptr__) if( __ptr__ != NULL) { sqlite3_finalize(__ptr__); }


@interface EventRecord()
@property(nonatomic,strong, readwrite) NSString *identifier;
@property(nonatomic,strong, readwrite) NSString *payload;
@end

@implementation EventRecord

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


@interface SQLiteStorage()
{
    sqlite3 *db;
    sqlite3_stmt *insertStatement;
    sqlite3_stmt *deleteStatement;
    sqlite3_stmt *retrieveStatement;
    sqlite3_stmt *countStatment;
    sqlite3_stmt *beginTransaction;
    sqlite3_stmt *commitTransaction;
}
@end

@implementation SQLiteStorage
- (instancetype)initWithDabasePath:(NSString *)path {
    self = [super init];
    if (self) {
        NSAssert([path length] != 0, @"");
        NSLog(@"%@", path);
        BOOL dbExist = [[NSFileManager defaultManager] fileExistsAtPath:path];
        int error = sqlite3_open_v2([path cStringUsingEncoding:NSUTF8StringEncoding], &db, SQLITE_OPEN_CREATE | SQLITE_OPEN_READWRITE | SQLITE_OPEN_FULLMUTEX, nil);
        if( error != SQLITE_OK) {
            return nil;
        }

        if( !dbExist) {
            error = sqlite3_exec(db, "CREATE TABLE events(id TEXT UNIQUE, data TEXT)",  nil, nil, nil);
            if( error != SQLITE_OK) {
                return nil;
            }
        }

        error = sqlite3_prepare_v2(db, "INSERT INTO events values(?,?)", -1, &insertStatement, nil);
        error = error == SQLITE_OK ? sqlite3_prepare_v2(db, "DELETE FROM events WHERE id IN ( ? )", -1, &deleteStatement, nil) : error;
        error = error == SQLITE_OK ? sqlite3_prepare_v2(db, "SELECT * FROM events LIMIT ?", -1, &retrieveStatement, nil) : error;
        error = error == SQLITE_OK ? sqlite3_prepare_v2(db, "SELECT COUNT(*) FROM events", -1, &countStatment, nil) : error;
         error = error == SQLITE_OK ? sqlite3_prepare_v2(db, "BEGIN TRANSACTION", -1, &beginTransaction, nil) : error;
         error = error == SQLITE_OK ? sqlite3_prepare_v2(db, "COMMIT TRANSACTION", -1, &commitTransaction, nil) : error;
        
        if (error != SQLITE_OK) {
            return  nil;
        }
    }
    return self;
}

- (void)dealloc {

    SQLITE_SAFE_FINALIZE(insertStatement)
    SQLITE_SAFE_FINALIZE(deleteStatement)
    SQLITE_SAFE_FINALIZE(countStatment)
    SQLITE_SAFE_FINALIZE(retrieveStatement)
    SQLITE_SAFE_FINALIZE(beginTransaction)
    SQLITE_SAFE_FINALIZE(commitTransaction)

    if(db != NULL) {
        sqlite3_close_v2(db);
    }
}

- (void)addEvent:(EventRecord *)record {
    BOOL success = YES;

    success = sqlite3_bind_text(insertStatement,1, [record.identifier UTF8String], -1, SQLITE_STATIC) == SQLITE_OK;
    success &= (sqlite3_bind_text(insertStatement,2, [record.payload UTF8String], -1, SQLITE_STATIC) == SQLITE_OK);
   success &= (sqlite3_step(insertStatement) == SQLITE_DONE);
    sqlite3_clear_bindings(insertStatement);
    sqlite3_reset(insertStatement);
    if (!success) {
        NSLog(@"Insert event statement failed");
    }
}

- (void)addEvents:(NSArray<EventRecord *> *)events {
    if( [self beginTransaction]) {
        for( EventRecord *record in events ){
            [self addEvent: record];
        }
        [self commitTransaction];
    }
}

- (void)removeEvents: (NSArray<NSString *> *)identifiers {
    if( [identifiers count] == 0 ){
        return;
    }

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
    NSString *query = [NSString stringWithFormat:@"DELETE FROM events WHERE id IN ( %@ )", ids];
    sqlite3_exec(db, [query UTF8String], nil, nil, nil);
//    success = (sqlite3_bind_text(deleteStatement,1, [ids UTF8String], -1, SQLITE_TRANSIENT) == SQLITE_OK);
//    NSLog(@"bind: %s, %d", sqlite3_errmsg(db), sqlite3_errcode(db));
//
//    success = sqlite3_step(deleteStatement);
    NSLog(@"%s, %d", sqlite3_errmsg(db), sqlite3_errcode(db));

    NSLog(@"Count after delete: %ld", (long)[self count]);

    sqlite3_clear_bindings(deleteStatement);
    sqlite3_reset(deleteStatement);
    if (!success) {
        NSLog(@"Delete events statement failed %d", sqlite3_errcode(db));
    }
}

- (NSArray<EventRecord *> *)nextEvents:(int)limit {
    if( limit < 1) {
        return nil;
    }

    NSMutableArray *events = [NSMutableArray array];

    int error = sqlite3_bind_int(retrieveStatement, 1, limit);
    while ((error = sqlite3_step(retrieveStatement)) == SQLITE_ROW) {
        const char * identifierCString = (const char*) sqlite3_column_text(retrieveStatement, 0);
        NSString *identifier = [NSString stringWithUTF8String:identifierCString];
        const char * payloadCString = (const char*) sqlite3_column_text(retrieveStatement, 1);
        NSString *payload = [NSString stringWithUTF8String:payloadCString];
        EventRecord *record = [[EventRecord alloc] initWithIdentifier:identifier payload:payload];
        if( record != nil) {
            [events addObject: record];
        }
    }

    sqlite3_clear_bindings(retrieveStatement);
    sqlite3_reset(retrieveStatement);

    return events;
}

- (NSInteger)count {
    int error = sqlite3_step(countStatment);
    NSInteger count = 0;
    if( error == SQLITE_ROW) {
        count = sqlite3_column_int(countStatment, 0);
    }

    sqlite3_clear_bindings(countStatment);
    sqlite3_reset(countStatment);
    return count;
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
