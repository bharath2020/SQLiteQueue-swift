//  Copyright © 2016 Atlassian. All rights reserved.

private class _AnyQueueBoxBase<Item> : Queue {
    
    typealias  Element = Item
    
    public var count: Int {
        fatalError()
    }

    func enqueue(_ item: Item) {
        fatalError()
    }
    
    func dequeue() -> Item? {
        fatalError()
    }
    
    func dequeue(count: Int) -> [Element]? {
        fatalError()
    }
    
    func peek(count: Int, deleteAfterPeek: Bool) -> [Element]? {
        fatalError()
    }
    
}


private class _AnyQueueBox<Base: Queue> : _AnyQueueBoxBase<Base.Element> {
    var base:Base
    init(_ base: Base){
        self.base = base
    }
    
    override var count: Int {
        return base.count
    }
    
    override func enqueue(_ item: Base.Element) {
        base.enqueue(item)
    }
    
    override func dequeue() -> Base.Element? {
        return base.dequeue()
    }
}

struct AnyQueue<T> : Queue {
   
    typealias Element = T
    
    fileprivate let box:_AnyQueueBoxBase<T>
    
    init<Q: Queue>(_ base: Q) where Q.Element == T {
        self.box = _AnyQueueBox(base)
    }
    
    public var count: Int {
        return box.count
    }
    
    public func enqueue(_ item: T) {
        box.enqueue(item)
    }
    
    func dequeue() -> T? {
        return box.dequeue()
    }
    
    func dequeue(count: Int) -> [T]? {
        return box.dequeue(count: count)
    }
    
    func peek(count: Int, deleteAfterPeek: Bool) -> [T]? {
        return box.peek(count: count, deleteAfterPeek: deleteAfterPeek )
    }

}
