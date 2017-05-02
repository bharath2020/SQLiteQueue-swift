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
    case incorrectDBPath(databasePath : String)
    case error(errorMessage: String)
    init(_ resultCode: Int32, _ dbHandle: OpaquePointer) {
        let msg = String(cString:sqlite3_errmsg(dbHandle))
        self = .error(errorMessage: "Error Code: \(resultCode) - \(msg))")
    }
}


public final class SQLiteQueue<T: StringConvertible> : Queue {
    public typealias Element = T
    
    private var _dbHandle: OpaquePointer? = nil
    private let sqliteSuccessCodes = [SQLITE_ROW, SQLITE_OK, SQLITE_DONE]
    private var dbHandle: OpaquePointer { return _dbHandle! }
    
    private var insertStmt: OpaquePointer? = nil
    private var deleteStmt: OpaquePointer? = nil
    private var retrieveStmt: OpaquePointer? = nil
    private var countStmt: OpaquePointer? = nil
    private let SQLITE_TRANSIENT = unsafeBitCast(-1, to: sqlite3_destructor_type.self)


    
    public init(_ databasePath : String) throws {
        
        guard !databasePath.isEmpty else {
            throw SQLiteQueueError.incorrectDBPath(databasePath: databasePath)
        }
        
        let dbExists = FileManager.default.fileExists(atPath: databasePath)
        if let error = test(sqlite3_open_v2(databasePath, &_dbHandle, SQLITE_OPEN_CREATE | SQLITE_OPEN_READWRITE | SQLITE_OPEN_FULLMUTEX, nil), [SQLITE_OK]) {
            throw error
        }
        
        if !dbExists , let error = test(sqlite3_exec(self.dbHandle, "CREATE TABLE queue(data TEXT);", nil, nil, nil))  {
            throw error
        }
        
       var error = test(sqlite3_prepare_v2(self.dbHandle, "INSERT INTO queue values(?)", -1, &insertStmt, nil))
           error = error ?? test(sqlite3_prepare_v2(self.dbHandle, "DELETE FROM queue LIMIT ?",-1, &deleteStmt, nil))
           error = error ?? test(sqlite3_prepare_v2(self.dbHandle, "SELECT data FROM queue LIMIT ?", -1, &retrieveStmt, nil))
           error = error ?? test(sqlite3_prepare_v2(self.dbHandle, "SELECT COUNT(data) FROM queue", -1, &countStmt, nil))
        
        guard (error == nil) else {
            fatalError("\(error!)")
        }
        
    }
    
    deinit {
        sqlite3_finalize(insertStmt)
        sqlite3_finalize(deleteStmt)
        sqlite3_finalize(retrieveStmt)
        sqlite3_finalize(countStmt)
        sqlite3_close_v2(self.dbHandle)
    }
    
    
    public func enqueue(_ item: Element) {
        defer {
            sqlite3_reset(insertStmt);
        }
        sqlite3_bind_text(insertStmt,1, item.toString(),-1,SQLITE_TRANSIENT)
        sqlite3_step(insertStmt)
        sqlite3_clear_bindings(insertStmt)
        sqlite3_reset(insertStmt)

    }
    
    public func peek(count: Int, deleteAfterPeek: Bool = false) -> [T]? {
        guard count >= 1 else {
            return nil
        }
        
        sqlite3_bind_int64(retrieveStmt, 1, Int64(count))
        var elements = [Element]()
        while sqlite3_step(retrieveStmt) == SQLITE_ROW {
            let cString = sqlite3_column_text(retrieveStmt, 0)
            let data = String(cString: cString!)
            if let element = Element(data) {
                elements.append(element)
            }
        }
        
        if deleteAfterPeek {
            sqlite3_bind_int64(deleteStmt, 1, Int64(count))
            sqlite3_step(deleteStmt)
            sqlite3_clear_bindings(deleteStmt)
            sqlite3_reset(deleteStmt)
        }
        
        sqlite3_clear_bindings(retrieveStmt)
        sqlite3_reset(retrieveStmt);
        return elements
    }
    
    public var count: Int {
        defer {
            sqlite3_reset(countStmt)
        }
        
        guard sqlite3_step(countStmt) == SQLITE_ROW else {
            return 0
        }
        
        let count = Int(sqlite3_column_int(countStmt, 0))
        
        return count
    }
    
    @discardableResult private func test(_ result: Int32, _ expectedCodes : [Int32] = []) -> SQLiteQueueError? {
        guard sqliteSuccessCodes.contains(result) || expectedCodes.contains(result) else {
            return SQLiteQueueError(result,self.dbHandle)
        }
        return nil
    }
}
