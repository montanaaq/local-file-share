using System;
using System.Text;
using Protocol;
using Protocol.Messages;

namespace Client;

public sealed class ClientSettings
{
    public string DeviceName { get; set; } = Environment.MachineName;
    public int LocalPort { get; set; } = ProtocolConstants.DefaultPort;
    public int LossRate { get; set; }
    public bool IsExchangeEnabled { get; set; } = true;

    public void Validate()
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(DeviceName);

        var utf8 = new UTF8Encoding(false, true);

        if (utf8.GetByteCount(DeviceName) > PayloadsConstants.MaxNameBytes)
        {
            throw new ArgumentException(
                "Имя устройства должно занимать не более 255 байт в UTF-8.",
                nameof(DeviceName)
            );
        }

        ArgumentOutOfRangeException.ThrowIfLessThan(LocalPort, 1);
        ArgumentOutOfRangeException.ThrowIfGreaterThan(LocalPort, 65535);
        ArgumentOutOfRangeException.ThrowIfNegative(LossRate);
        ArgumentOutOfRangeException.ThrowIfGreaterThan(LossRate, 100);
    }
}
