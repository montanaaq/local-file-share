using System.Globalization;

namespace Client;

internal static class FileSizeExtensions
{
    internal static string ToFileSize(this ulong size)
    {
        double divisor =
            size >= 1024 * 1024 ? 1024.0 * 1024
            : size >= 1024 ? 1024
            : 1;

        string unit =
            size >= 1024 * 1024 ? "МБ"
            : size >= 1024 ? "КБ"
            : "Б";

        string value = (size / divisor).ToString(
            divisor == 1 ? "0" : "0.#",
            CultureInfo.GetCultureInfo("ru-RU")
        );

        return $"{value} {unit}";
    }
}
