using System.Net.Sockets;

namespace Protocol;

internal static class SocketExceptionExtensions
{
    internal static bool IsUnreachable(this SocketException exception) =>
        exception.SocketErrorCode
            is SocketError.NetworkUnreachable
                or SocketError.HostUnreachable
                or SocketError.NetworkDown
                or SocketError.AddressNotAvailable
                or SocketError.ConnectionRefused;
}
