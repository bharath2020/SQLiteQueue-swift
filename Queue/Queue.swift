//  Copyright © 2016 Atlassian. All rights reserved.

protocol AnyQueueConvertible  {
    associatedtype T
    func anyQueue() -> AnyQueue<T>
}


public protocol Queue  {
    associatedtype Element
    var isEmpty: Bool { get }
    var count: Int { get }
    mutating func enqueue(_ item: Element)
    mutating func dequeue() -> Element?
    mutating func dequeue(count: Int) -> [Element]?
    func peek() -> Element?
    func peek(count: Int, deleteAfterPeek: Bool) -> [Element]?
}

extension Queue {
    
    public var isEmpty : Bool {
        return self.count == 0
    }
    
    public mutating func dequeue(count: Int) -> [Element]? {
        return peek(count: count, deleteAfterPeek: true)
    }
    
    public mutating func dequeue() -> Element? {
        guard  let elements = dequeue(count: 1) , elements.count == 1 else {
            return nil
        }
        return elements[0]
    }
    
    public func peek() -> Element? {
        guard let elements = peek(count: 1, deleteAfterPeek: false), elements.count == 1 else {
            return nil
        }
        return elements[0]
    }
}
