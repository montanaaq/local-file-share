using System;
using System.IO;
using System.Text;

namespace Protocol;

internal static class FileNameExtensions
{
    /// <summary>
    /// Заменяет управляющие и запрещённые в Windows символы на подчёркивания,
    /// удаляет пробелы и точки по краям и изменяет зарезервированные имена устройств.
    /// Если после очистки имя пустое, возвращает "file". Уникальность имени не проверяет.
    /// </summary>
    internal static string SanitizeFileName(this string fileName)
    {
        ArgumentNullException.ThrowIfNull(fileName);
        var chars = fileName.ToCharArray();
        const string forbidden = "<>:\"/\\|?*";
        for (var index = 0; index < chars.Length; index++)
        {
            if (chars[index] <= '\u001f' || forbidden.Contains(chars[index]))
                chars[index] = '_';
        }

        var name = new string(chars).Trim(' ', '.');
        if (name.Length == 0)
            return "file";

        var dotIndex = name.IndexOf('.');
        var stem = (dotIndex < 0 ? name : name[..dotIndex]).TrimEnd(' ').ToUpperInvariant();
        var reservedName = stem is "CON" or "PRN" or "AUX" or "NUL";
        var reservedPort =
            stem.Length == 4
            && (stem[..3] is "COM" or "LPT")
            && stem[3] is >= '1' and <= '9' or '¹' or '²' or '³';

        return reservedName || reservedPort ? "_" + name[1..] : name;
    }

    /// <summary>
    /// Добавляет к очищенному имени суффикс " (number)" перед расширением;
    /// при нуле возвращает имя без суффикса. При необходимости укорачивает имя,
    /// включая расширение, чтобы результат укладывался в лимит
    /// <see cref="PayloadsConstants.MaxNameBytes"/> байт UTF-8,
    /// не разрывая суррогатные пары. Наличие файла с таким именем не проверяет.
    /// </summary>
    internal static string WithFileSuffix(this string fileName, int number)
    {
        ArgumentNullException.ThrowIfNull(fileName);
        ArgumentOutOfRangeException.ThrowIfNegative(number);

        var suffix = number == 0 ? string.Empty : $" ({number})";
        var shortenedName = fileName;
        var suffixBytes = Encoding.UTF8.GetByteCount(suffix);

        while (
            Encoding.UTF8.GetByteCount(shortenedName) + suffixBytes
            > PayloadsConstants.MaxNameBytes
        )
        {
            var removeCount = char.IsLowSurrogate(shortenedName[^1]) ? 2 : 1;
            shortenedName = shortenedName[..^removeCount];
        }

        var name = Path.GetFileNameWithoutExtension(shortenedName);
        var extension = Path.GetExtension(shortenedName);

        return $"{name}{suffix}{extension}";
    }
}
