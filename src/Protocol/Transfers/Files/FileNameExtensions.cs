using System;
using System.IO;
using System.Text;

namespace Protocol;

internal static class FileNameExtensions
{
    /// <summary>
    /// Заменяет запрещённые в Windows символы и зарезервированные имена, удаляет крайние точки и пробелы.
    /// </summary>
    /// <param name="fileName">Полученное от удалённого устройства имя; разделители пути также заменяются.</param>
    /// <returns>Безопасное имя либо file для пустого результата. Наличие такого файла на диске не проверяется.</returns>
    internal static string SanitizeFileName(this string fileName)
    {
        ArgumentNullException.ThrowIfNull(fileName);

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
    /// Добавляет суффикс перед расширением и ограничивает длину 255 байтами UTF-8 без разрыва суррогатной пары.
    /// </summary>
    /// <param name="fileName">Уже очищенное имя файла.</param>
    /// <param name="number">Номер коллизии: 0 не добавляет суффикс, положительное значение добавляет (number).</param>
    /// <returns>Имя подходящей длины; существование файла и уникальность проверяются отдельно.</returns>
    internal static string WithFileSuffix(this string fileName, int number)
    {
        ArgumentNullException.ThrowIfNull(fileName);

        ArgumentOutOfRangeException.ThrowIfNegative(number);

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
