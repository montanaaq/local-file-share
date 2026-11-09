using Protocol;

namespace Client;

public sealed record IncomingFileOffer(
    TransferKey Key,
    string FileName,
    ulong FileSize,
    string SenderName
)
{
    public string FormattedSize => FileSize.ToFileSize();
}
