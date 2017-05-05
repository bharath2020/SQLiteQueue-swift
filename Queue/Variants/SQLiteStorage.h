//  Copyright © 2017 Atlassian. All rights reserved.
//

#import <Foundation/Foundation.h>

@interface Record : NSObject
@property(nonatomic,strong, readonly) NSString * _Nonnull identifier;
@property(nonatomic,strong, readonly) NSString * _Nonnull payload;

- (instancetype _Nullable)initWithIdentifier:(NSString * _Nonnull)identifier payload:(NSString * _Nonnull)payload;
@end

@interface SQLiteStorage : NSObject
- (instancetype _Nullable)initWithDabasePath:(NSString * _Nullable)path;
- (void)addEvent:(Record * _Nonnull)record NS_SWIFT_NAME(add(event:));
- (void)removeEvents: (NSArray<NSString *> * _Nonnull)identifiers NS_SWIFT_NAME(remove(events:));
- (void)nextEvents:(int)limit resultBlock: (void (^ _Nonnull)(NSArray<Record *> * _Nullable records))resultBlock NS_SWIFT_NAME(nextEvents(limit:resultBlock:));
- (void)count: (void (^ _Nonnull)(NSInteger count))resultBlock NS_SWIFT_NAME(count(resultBlock:));
@end
