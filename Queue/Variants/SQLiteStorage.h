//  Copyright © 2017 Atlassian. All rights reserved.
//

#import <Foundation/Foundation.h>

@interface Record : NSObject
@property(nonatomic,strong, readonly) NSString *identifier;
@property(nonatomic,strong, readonly) NSString *payload;

- (instancetype)initWithIdentifier:(NSString *)identifier payload:(NSString *)payload;
@end

@interface SQLiteStorage : NSObject
- (instancetype)initWithDabasePath:(NSString *)path;
- (void)addEvent:(Record *)record NS_SWIFT_NAME(add(event:));
- (void)removeEvents: (NSArray<NSString *> *)identifiers NS_SWIFT_NAME(remvove(events:));
- (NSArray<Record *> *)nextEvents:(int)limit NS_SWIFT_NAME(nextEvents(limit:));
- (NSInteger)count NS_SWIFT_NAME(count());
@end
