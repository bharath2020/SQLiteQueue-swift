//  Copyright © 2016 Atlassian. All rights reserved.
//

import XCTest
@testable import Queue

class QueueTests: XCTestCase {
    
    var sqlQueue: SQLiteQueue<String>?
    override func setUp() {
        super.setUp()
         let docDir = NSSearchPathForDirectoriesInDomains(.documentDirectory, .userDomainMask, true)[0]
         let dbFile =  docDir + "/new.db"
         try? FileManager.default.removeItem(atPath: dbFile)
         self.sqlQueue = try? SQLiteQueue<String>(dbFile)
    }
    
    private var randomDBPath : String {
        let docDir = NSSearchPathForDirectoriesInDomains(.documentDirectory, .userDomainMask, true)[0]
        return docDir + UUID().uuidString
    }
    
    override func tearDown() {
        // Put teardown code here. This method is called after the invocation of each test method in the class.
        super.tearDown()
    }


    func testInsertAndDeleteOneElementEachTime() {
        XCTAssertNotNil(sqlQueue)
        sqlQueue?.enqueue("Hello")
        XCTAssertEqual(1, sqlQueue?.count)
        XCTAssertEqual("Hello", sqlQueue?.peek())
        XCTAssertEqual("Hello", sqlQueue?.dequeue())
        XCTAssertEqual(0, sqlQueue?.count)
        XCTAssertNil(sqlQueue?.peek())
    }

    func testInsertOneElementButDeleteMultipleElementsEachTime() {
        for x in 1...100 {
            sqlQueue?.enqueue("\(x)")
        }
        XCTAssertEqual(100, sqlQueue?.count)
        XCTAssertEqual(sqlQueue?.dequeue(count: 200)?.count, 100)
        XCTAssertEqual(sqlQueue?.count, 0)
        XCTAssertNil(sqlQueue?.dequeue())

    }
    
    func testUnicodeCharacter() {
        sqlQueue?.enqueue("😀")
        XCTAssertEqual("😀", sqlQueue?.peek())
    }
    
    func testEscapeCharacters() {
        sqlQueue?.enqueue("\"';")
        XCTAssertEqual(sqlQueue?.peek(),"\"';")
    }
    
    func testPreserveContentOnMultipleOpenAndClose() {
        let dbPath = self.randomDBPath
        var queue = try? SQLiteQueue<String>(dbPath)
        for x in 1...100 {
            queue?.enqueue("\(x)")
        }
        
        queue = try? SQLiteQueue<String>(dbPath)
        XCTAssertEqual(100, queue?.count)
    }
    
}
