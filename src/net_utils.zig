const std = @import("std");
const Io = std.Io;
const net = std.Io.net;

pub fn parseLocation(loc: []const u8) !net.IpAddress {
    const colon = std.mem.lastIndexOfScalar(u8, loc, ':');
    const host = loc[0..(colon orelse (loc.len - 1))];
    const port_str: []const u8 = if (colon) |idx| loc[idx + 1 ..] else "0";
    const port = try std.fmt.parseUnsigned(u16, port_str, 10);

    return try parseAddress(host, port);
}

fn parseAddress(text: []const u8, port: u16) !net.IpAddress {
    const address = net.IpAddress.parse(text, port) catch
        net.IpAddress{ .ip4 = net.Ip4Address.loopback(port) };
    return address;
}

pub fn connectAddress(io: Io, address: net.IpAddress) !net.Stream {
    const stream = address.connect(io, .{
        .mode = .stream,
        .protocol = .tcp,
    }) catch |err| {
        std.debug.print("connection to target failed: {f}: {s}\n\n", .{ address, @errorName(err) });
        return err;
    };

    return stream;
}
