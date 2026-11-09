using System;
using System.Net;
using CommunityToolkit.Mvvm.ComponentModel;
using Protocol;

namespace Client;

public partial class PeerItem : ObservableObject
{
    public PeerItem(Peer snapshot)
    {
        Snapshot = snapshot;
    }

    [ObservableProperty]
    [NotifyPropertyChangedFor(nameof(Name), nameof(Endpoint), nameof(Availability))]
    public partial Peer Snapshot { get; set; }

    public Guid DeviceId => Snapshot.DeviceId;
    public string Name => Snapshot.Name;
    public IPEndPoint Endpoint => Snapshot.Endpoint;
    public string Availability =>
        Snapshot.Availability switch
        {
            PeerAvailability.Available => "Доступно",
            PeerAvailability.Unreachable => "Нет связи",
            PeerAvailability.Offline => "Не в сети",
            _ => "Неизвестное состояние",
        };
}
