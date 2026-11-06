using System.Globalization;
using Protocol;

namespace Client;

public sealed record IncomingFileOffer(
    TransferKey Key,
    string FileName,
    ulong FileSize,
    string SenderName
)
{
    public string FormattedSize
    {
        get
        {
            bool useMegabytes = FileSize >= 1024 * 1024;
            double size = FileSize / (useMegabytes ? 1024.0 * 1024 : 1024.0);
            string unit = useMegabytes ? "МБ" : "КБ";
            return $"{size.ToString("F1", CultureInfo.GetCultureInfo("ru-RU"))} {unit}";
        }
    }
}
