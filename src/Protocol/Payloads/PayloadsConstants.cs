namespace Protocol;

public static class PayloadsConstants
{
    public const int PeerIdSize = 2;
    public const int FileIdSize = 16;
    public const int FileSizeSize = 8;
    public const int Sha256Size = 32;
    public const int NameLengthSize = 1;
    public const int MaxNameBytes = byte.MaxValue;

    public const int IndexSize = 4;
    public const int IndexOffset = FileIdOffset + FileIdSize;
    public const int ChunkDataOffset = IndexOffset + IndexSize;

    public const int FileIdOffset = PeerIdSize;
    public const int FileSizeOffset = FileIdOffset + FileIdSize;
    public const int Sha256Offset = FileSizeOffset + FileSizeSize;
    public const int NameLengthOffset = Sha256Offset + Sha256Size;
    public const int NameOffset = NameLengthOffset + NameLengthSize;
}
