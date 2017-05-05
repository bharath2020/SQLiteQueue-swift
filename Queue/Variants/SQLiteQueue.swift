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
        let nextRecords: [Record]? = DispatchQueue.global().sync {
            let semaphore = DispatchSemaphore(value: 0)
            var nextRecords: [Record]?
            db.nextEvents(limit: Int32(count)) { records in
                nextRecords = records
                semaphore.signal()
            }
            semaphore.wait()
            return nextRecords
        }
        guard let records = nextRecords else {
            return nil
        }

        let items = records.flatMap {
            T($0.payload)
        }

        if deleteAfterPeek {
            let ids = records.flatMap {
                $0.identifier
            }
            db.remove(events: ids)
        }
        return items
    }
    
    public var count: Int {
       return DispatchQueue.global().sync {
            let semaphore = DispatchSemaphore(value: 0)
            var totalRecords: Int = 0
            db.count { total in
                totalRecords  = total
                semaphore.signal()
            }

            semaphore.wait()
            return totalRecords
        }
    }
    

}
