namespace Protocol;

public static class PayloadsConstants
{
    public const int DeviceIdSize = 16;
    public const int FileIdSize = 16;
    public const int FileIdOffset = DeviceIdSize;
    public const int FileSizeSize = 8;
    public const int Sha256Size = 32;
    public const int NameLengthSize = 1;
    public const int MaxNameBytes = byte.MaxValue;

    // ACK
    public const int AckedSeqOffset = DeviceIdSize;
    public const int AckSize = AckedSeqOffset + sizeof(uint);

    // CHUNK
    public const int IndexSize = 4;
    public const int IndexOffset = FileIdOffset + FileIdSize;
    public const int ChunkDataOffset = IndexOffset + IndexSize;

    // FILE_OFFER
    public const int FileSizeOffset = FileIdOffset + FileIdSize;
    public const int Sha256Offset = FileSizeOffset + FileSizeSize;
    public const int NameLengthOffset = Sha256Offset + Sha256Size;
    public const int NameOffset = NameLengthOffset + NameLengthSize;

    // PEER_INFO
    public const int PeerNameLengthOffset = DeviceIdSize;
    public const int PeerNameOffset = PeerNameLengthOffset + NameLengthSize;

    // FILE_ANSWER
    public const int AcceptedOffset = FileIdOffset + FileIdSize;
    public const int NextIndexOffset = AcceptedOffset + sizeof(byte);
    public const int FileAnswerSize = NextIndexOffset + sizeof(uint);

    // FILE_DONE
    public const int FileDoneSize = FileIdOffset + FileIdSize;

    // FILE_RESULT
    public const int OkOffset = FileIdOffset + FileIdSize;
    public const int FileResultSize = OkOffset + sizeof(byte);

    // CLIPBOARD
    public const int MinClipboardPayloadSize = DeviceIdSize;
    public const int MaxClipboardPayloadSize = DeviceIdSize + ProtocolConstants.ClipboardTextSize;
}
