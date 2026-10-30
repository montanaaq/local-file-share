using System;

namespace Client;

public sealed class ClientSettings
{
    public string DeviceName { get; set; } = Environment.MachineName;
    public int LocalPort { get; set; } = 5000;
}
