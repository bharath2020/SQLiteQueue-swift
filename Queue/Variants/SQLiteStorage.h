//
//  SQLiteStorage.h
//  Queue
//
//  Created by Bharath Booshan on 5/3/17.
//  Copyright © 2017 Atlassian. All rights reserved.
//

#import <Foundation/Foundation.h>

@interface EventRecord : NSObject
@property(nonatomic,strong, readonly) NSString *identifier;
@property(nonatomic,strong, readonly) NSString *payload;

- (instancetype)initWithIdentifier:(NSString *)identifier payload:(NSString *)payload;
@end

@interface SQLiteStorage : NSObject
- (instancetype)initWithDabasePath:(NSString *)path;
- (void)addEvent:(EventRecord *)record NS_SWIFT_NAME(add(event:));
- (void)addEvents:(NSArray<EventRecord *> *)events NS_SWIFT_NAME(add(events:));
- (void)removeEvents: (NSArray<NSString *> *)identifiers NS_SWIFT_NAME(remvove(events:));
- (NSArray<EventRecord *> *)nextEvents:(int)limit NS_SWIFT_NAME(nextEvents(limit:));
- (NSInteger)count NS_SWIFT_NAME(count());
@end
