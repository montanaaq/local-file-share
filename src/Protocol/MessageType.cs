namespace Protocol;

public enum MessageType
{
    Hello = 0x01,
    Heartbeat = 0x02,
    Ack = 0x03,
    Error = 0x04,

    PeerList = 0x10,

    FileOffer = 0x20,
    FileAnswer = 0x21,

    Chunk = 0x30,
    FileDone = 0x31,
    FileResult = 0x32,

    Clipboard = 0x40,
}
