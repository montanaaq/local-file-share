namespace Protocol;

public enum MessageType
{
    Heartbeat = 0x02,
    Ack = 0x03,
    Discover = 0x05,
    PeerInfo = 0x06,

    FileOffer = 0x20,
    FileAnswer = 0x21,

    Chunk = 0x30,
    FileDone = 0x31,
    FileResult = 0x32,

    Clipboard = 0x40,
}
