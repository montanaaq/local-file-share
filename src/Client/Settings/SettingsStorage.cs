using System;
using System.IO;
using System.Text.Json;
using Client;

namespace Models;

public sealed class SettingsStorage
{
    private readonly string _filePath;

    public SettingsStorage(string profile)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(profile);
        foreach (char character in profile)
        {
            if (!char.IsAsciiLetterOrDigit(character) && character != '-' && character != '_')
            {
                throw new ArgumentException(
                    "Profile may only contain ASCII letters, digits",
                    nameof(profile)
                );
            }
        }

        string directory = Path.Combine(
            Environment.GetFolderPath(Environment.SpecialFolder.ApplicationData),
            "LocalFileShare"
        );
        _filePath = Path.Combine(directory, $"{profile}.json");
    }

    public ClientSettings Load()
    {
        if (!File.Exists(_filePath))
        {
            return new ClientSettings();
        }
        string json = File.ReadAllText(_filePath);

        return JsonSerializer.Deserialize<ClientSettings>(json)
            ?? throw new InvalidDataException("Settings file is empty");
    }

    public void Save(ClientSettings settings)
    {
        ArgumentNullException.ThrowIfNull(settings);

        string directory =
            Path.GetDirectoryName(_filePath)
            ?? throw new InvalidOperationException("Settings directory is missing");

        Directory.CreateDirectory(directory);

        string json = JsonSerializer.Serialize(
            settings,
            new JsonSerializerOptions { WriteIndented = true }
        );

        File.WriteAllText(_filePath, json);
    }
}
