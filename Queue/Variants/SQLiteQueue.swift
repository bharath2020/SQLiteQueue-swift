//  Copyright © 2016 Atlassian. All rights reserved.

import Foundation

public protocol StringConvertible {
    init?(_ str: String)
    func toString() -> String
}

extension String : StringConvertible {
    public func toString() -> String {
        return self
    }
}

public enum SQLiteQueueError : Error {
    case cannotReadDBFile
}


public final class SQLiteQueue<T: StringConvertible> : Queue {

    let db: SQLiteStorage
    public init(_ databasePath : String) throws {
        guard let db = SQLiteStorage(dabasePath: databasePath) else {
            throw SQLiteQueueError.cannotReadDBFile
        }

        self.db = db
    }
    

    public func enqueue(_ item: T) {
        guard let record = Record(identifier: UUID().uuidString, payload: item.toString()) else {
            return
        }
        db.add(event: record)
    }
    
    public func peek(count: Int, deleteAfterPeek: Bool = false) -> [T]? {
        guard let records = db.nextEvents(limit: Int32(count)) else {
            return nil
        }

        let items = records.flatMap {
            T($0.payload)
        }

        if deleteAfterPeek {
            let ids = records.flatMap {
                $0.identifier
            }
            db.remvove(events: ids)
        }
        return items
    }
    
    public var count: Int {
        return db.count()
    }
    

}
