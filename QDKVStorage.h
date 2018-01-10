#import <Foundation/Foundation.h>

#define KVS_ENABLE_THREAD_SAFE      1
#define KVS_ENABLE_ERROR_LOGS       1

@interface QDKVStorage : NSObject

#pragma mark - Initializer

- (nullable instancetype)init UNAVAILABLE_ATTRIBUTE;
+ (nullable instancetype)new UNAVAILABLE_ATTRIBUTE;

- (nullable instancetype)initWithPath:(nullable NSString*)path NS_DESIGNATED_INITIALIZER;

#pragma mark - Access value

- (BOOL)setValue:(nullable id<NSCoding>)value forKey:(nullable NSString*)key;

- (BOOL)removeValueForKey:(nullable NSString*)key;

- (BOOL)removeValuesForKeys:(nullable NSArray<NSString*>*)keys;

- (BOOL)removeAllValues;

- (nullable id<NSCoding>)valueForKey:(nullable NSString*)key;

- (BOOL)valueExistsForKey:(nullable NSString*)key;

- (nullable NSArray<id<NSCoding>>*)getAllValues;

- (NSInteger)getAllValuesCount;

- (NSInteger)getValuesTotalSize;

@end
