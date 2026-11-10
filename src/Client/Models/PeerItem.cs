using System;
using System.Net;
using CommunityToolkit.Mvvm.ComponentModel;
using Protocol.Peers;

namespace Client;

public partial class PeerItem(Peer snapshot) : ObservableObject
{
    [ObservableProperty]
    [NotifyPropertyChangedFor(
        nameof(Name),
        nameof(Endpoint),
        nameof(Availability),
        nameof(IsAvailable)
    )]
    public partial Peer Snapshot { get; set; } = snapshot;

    public Guid DeviceId => Snapshot.DeviceId;
    public string Name => Snapshot.Name;
    public IPEndPoint Endpoint => Snapshot.Endpoint;
    public bool IsAvailable => Snapshot.Availability == PeerAvailability.Available;
    public string Availability =>
        Snapshot.Availability switch
        {
            PeerAvailability.Available => "Доступно",
            PeerAvailability.Unreachable => "Нет связи",
            PeerAvailability.Offline => "Не в сети",
            _ => "Неизвестное состояние",
        };
}
