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
    public PeerAvailability Availability => Snapshot.Availability;
}
