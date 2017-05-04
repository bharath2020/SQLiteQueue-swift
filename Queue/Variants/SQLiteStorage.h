//
//  SQLiteStorage.h
//  Queue
//
//  Created by Bharath Booshan on 5/3/17.
//  Copyright © 2017 Atlassian. All rights reserved.
//

#import <Foundation/Foundation.h>

@interface SQLiteStorage : NSObject
- (instancetype)initWithDabasePath:(NSString *)path;
@end

@interface EventRecord : NSObject {
 
}

- (instancetype)initWithIdentifier:(NSString *)identifier payload:(NSString *)payload;

@end
