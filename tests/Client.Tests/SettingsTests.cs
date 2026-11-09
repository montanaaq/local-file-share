using System.Text.Json;
using Client;
using Client.ViewModels;

namespace Client.Tests;

public class SettingsTests
{
    [Theory]
    [InlineData(0)]
    [InlineData(25)]
    [InlineData(100)]
    public void ConnectionBreak_RestoresConfiguredLoss_WithoutSavingSimulatedOutage(int loss)
    {
        var viewModel = new MainViewModel($"test-{Guid.NewGuid():N}")
        {
            LossRate = loss,
            IsConnectionBroken = true,
        };
        Assert.Equal(100, viewModel.LossRate);
        Assert.Equal(loss, viewModel.ClientSettings.LossRate);
        viewModel.LossRate = 10;
        Assert.Equal(100, viewModel.LossRate);

        viewModel.IsConnectionBroken = false;
        Assert.Equal(loss, viewModel.LossRate);
    }

    [Fact]
    public void Settings_KeepNetworkPreferences_AndReadOlderProfiles()
    {
        var viewModel = new MainViewModel($"test-{Guid.NewGuid():N}")
        {
            LossRate = 30,
            IsExchangeEnabled = false,
        };
        string json = JsonSerializer.Serialize(viewModel.ClientSettings);
        var restored = JsonSerializer.Deserialize<ClientSettings>(json)!;
        restored.Validate();
        Assert.Equal(30, restored.LossRate);
        Assert.False(restored.IsExchangeEnabled);

        var older = JsonSerializer.Deserialize<ClientSettings>(
            "{\"DeviceName\":\"Peer\",\"LocalPort\":5001}"
        )!;
        older.Validate();
        Assert.Equal(0, older.LossRate);
        Assert.True(older.IsExchangeEnabled);
    }

    [Theory]
    [InlineData("", 5000, 0)]
    [InlineData(" ", 5000, 0)]
    [InlineData(null, 5000, 0)]
    [InlineData("Peer", 0, 0)]
    [InlineData("Peer", 65536, 0)]
    [InlineData("Peer", 5000, -1)]
    [InlineData("Peer", 5000, 101)]
    public void Validate_RejectsInvalidSettings(string? name, int port, int loss)
    {
        var settings = new ClientSettings
        {
            DeviceName = name!,
            LocalPort = port,
            LossRate = loss,
        };
        Assert.ThrowsAny<ArgumentException>(settings.Validate);
    }

    [Fact]
    public void Validate_MeasuresNameInUtf8Bytes()
    {
        new ClientSettings { DeviceName = new string('я', 127) }.Validate();
        var settings = new ClientSettings { DeviceName = new string('я', 128) };
        Assert.Throws<ArgumentException>(settings.Validate);
    }

    [Fact]
    public void Validate_RejectsInvalidUnicodeBeforeStartingSession()
    {
        var settings = new ClientSettings { DeviceName = "\ud800" };
        Assert.Throws<System.Text.EncoderFallbackException>(settings.Validate);
    }
}
