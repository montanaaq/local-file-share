using System.Text;

namespace Protocol.Tests;

public class FileNameExtensionsTests
{
    [Theory]
    [InlineData("report.pdf", "report.pdf")]
    [InlineData("отчёт.txt", "отчёт.txt")]
    [InlineData("a:b?.txt", "a_b_.txt")]
    [InlineData("<>:\"/\\|?*", "_________")]
    [InlineData("a\0b\u001f.txt", "a_b_.txt")]
    [InlineData("../../report.txt", "_.._report.txt")]
    [InlineData("  report.txt. ", "report.txt")]
    [InlineData("...", "file")]
    [InlineData("", "file")]
    [InlineData("con.txt", "_on.txt")]
    [InlineData("NUL.tar.gz", "_UL.tar.gz")]
    [InlineData("CON .txt", "_ON .txt")]
    [InlineData("PRN", "_RN")]
    [InlineData("AUX", "_UX")]
    [InlineData("COM1.bin", "_OM1.bin")]
    [InlineData("lpt9.txt", "_pt9.txt")]
    [InlineData("COM¹.txt", "_OM¹.txt")]
    [InlineData("LPT².txt", "_PT².txt")]
    [InlineData("LPT³", "_PT³")]
    [InlineData("COM10.txt", "COM10.txt")]
    [InlineData("console.txt", "console.txt")]
    public void SanitizeFileName_ReturnsSafeName(string fileName, string expected)
    {
        string result = fileName.SanitizeFileName();

        Assert.Equal(expected, result);
    }

    [Theory]
    [InlineData("report.pdf", 0, "report.pdf")]
    [InlineData("report.pdf", 1, "report (1).pdf")]
    [InlineData("report.pdf", 2, "report (2).pdf")]
    [InlineData("report", 1, "report (1)")]
    [InlineData("archive.tar.gz", 1, "archive.tar (1).gz")]
    public void WithFileSuffix_AddsSuffixBeforeExtension(
        string fileName,
        int number,
        string expected
    )
    {
        string result = fileName.WithFileSuffix(number);

        Assert.Equal(expected, result);
    }

    [Theory]
    [InlineData("я")]
    [InlineData("😀")]
    public void WithFileSuffix_LimitsUtf8Length_WithoutSplittingUnicodeCharacters(string character)
    {
        var utf8 = new UTF8Encoding(false, true);
        int count = PayloadsConstants.MaxNameBytes / utf8.GetByteCount(character);
        string fileName = string.Concat(Enumerable.Repeat(character, count));
        string suffix = " (1)";

        string result = fileName.WithFileSuffix(1);

        int expectedCount =
            (PayloadsConstants.MaxNameBytes - utf8.GetByteCount(suffix))
            / utf8.GetByteCount(character);
        Assert.Equal(string.Concat(Enumerable.Repeat(character, expectedCount)) + suffix, result);
        Assert.True(utf8.GetByteCount(result) <= PayloadsConstants.MaxNameBytes);
    }
}
