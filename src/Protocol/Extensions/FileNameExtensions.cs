using System;
using System.IO;
using System.Text;

namespace Protocol;

internal static class FileNameExtensions
{
    /// <summary>
    /// Replaces control characters and characters forbidden on Windows with underscores,
    /// trims surrounding spaces and dots, and changes reserved device names.
    /// Returns "file" if the sanitized name is empty. Does not check uniqueness.
    /// </summary>
    internal static string SanitizeFileName(this string fileName)
    {
        if (fileName is null)
        {
            throw new ArgumentNullException(nameof(fileName), "Value cannot be null");
        }

        char[] chars = fileName.ToCharArray();
        const string forbidden = "<>:\"/\\|?*";
        for (int index = 0; index < chars.Length; index++)
        {
            if (chars[index] <= '\u001f' || forbidden.Contains(chars[index]))
            {
                chars[index] = '_';
            }
        }

        string name = new string(chars).Trim(' ', '.');
        if (name.Length == 0)
        {
            return "file";
        }

        int dotIndex = name.IndexOf('.');
        string stem = (dotIndex < 0 ? name : name[..dotIndex]).TrimEnd(' ').ToUpperInvariant();
        bool reservedName = stem is "CON" or "PRN" or "AUX" or "NUL";
        bool reservedPort =
            stem.Length == 4
            && (stem[..3] is "COM" or "LPT")
            && stem[3] is >= '1' and <= '9' or '¹' or '²' or '³';

        return reservedName || reservedPort ? "_" + name[1..] : name;
    }

    /// <summary>
    /// Adds a " (number)" suffix before the sanitized file extension;
    /// zero leaves the name unchanged. Truncates the name when necessary,
    /// including the extension, to fit within
    /// <see cref="PayloadsConstants.MaxNameBytes"/> UTF-8 bytes
    /// without splitting surrogate pairs. Does not check whether the file exists.
    /// </summary>
    internal static string WithFileSuffix(this string fileName, int number)
    {
        if (fileName is null)
        {
            throw new ArgumentNullException(nameof(fileName), "Value cannot be null");
        }

        if (number < 0)
        {
            throw new ArgumentOutOfRangeException(
                nameof(number),
                number,
                "Suffix number cannot be negative"
            );
        }

        string suffix = number == 0 ? string.Empty : $" ({number})";
        string shortenedName = fileName;
        int suffixBytes = Encoding.UTF8.GetByteCount(suffix);

        while (
            Encoding.UTF8.GetByteCount(shortenedName) + suffixBytes > PayloadsConstants.MaxNameBytes
        )
        {
            int removeCount = char.IsLowSurrogate(shortenedName[^1]) ? 2 : 1;
            shortenedName = shortenedName[..^removeCount];
        }

        string name = Path.GetFileNameWithoutExtension(shortenedName);
        string extension = Path.GetExtension(shortenedName);

        return $"{name}{suffix}{extension}";
    }
}
