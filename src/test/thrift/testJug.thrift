namespace java org.breizhbeans.thrift.tools.thriftmongobridge.jug.test

struct Message {
        1:i64           date,
        2:string        talker,
        3:list<string>  readedBy,
        4:string        subject,
        5:string        content,
}

struct Conversation {
        1:i32    id,
        2:list<string>  contributors,
        3:list<string>  tags,
        4:i32           adId,
        5:list<Message> messages,
}