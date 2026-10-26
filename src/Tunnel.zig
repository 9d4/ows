const std = @import("std");
const Io = std.Io;
const net = std.Io.net;

const httpz = @import("httpz");

const StreamResult = union(enum) {
    rx: Io.Cancelable!void,
    tx: Io.Cancelable!void,
};

fn pipeStream(reader: *std.Io.Reader, writer: *std.Io.Writer) void {
    _ = reader.streamRemaining(writer) catch |err| {
        std.debug.print("error streaming reader: {s}\n", .{@errorName(err)});
    };
}

pub fn pipeNetStreamToWS(io: Io, from: net.Stream, ws: *httpz.websocket.Conn) !void {
    var buf: [1]u8 = undefined;
    var reader = from.reader(io, &buf);

    while (true) {
        const b = try reader.interface.takeByte();
        std.debug.print("READ from net peer stream\n", .{});
        try ws.write(&[_]u8{b});
    }

    return;
}

fn serveConnect(io: Io, addr: net.IpAddress) !net.Stream {
    const stream = addr.connect(io, .{ .mode = .stream, .protocol = .tcp });
    return stream;
}

fn serveListen(io: Io, addr: net.IpAddress, peer_address: net.IpAddress) !void {
    var srv = try addr.listen(io, .{ .reuse_address = true });
    defer srv.deinit(io);

    std.debug.print("listening on {f}\n", .{srv.socket.address});

    while (true) {
        std.debug.print("Waiting client...\n", .{});

        const stream = try srv.accept(io);

        std.debug.print("Client connected, trying connection to peer\n", .{});
        const peer = peer_address.connect(io, .{
            .mode = .stream,
            .protocol = .tcp,
        }) catch |err| {
            std.debug.print("connection to target failed: {f}: {s}\n\n", .{ peer_address, @errorName(err) });

            var w = stream.writer(io, &.{});
            _ = w.interface.write("connection to target failed") catch {};
            _ = w.interface.flush() catch {};
            stream.close(io);

            continue;
        };
        std.debug.print("connected to peer!\n", .{});

        serveListenHandle(io, stream, peer) catch continue;
    }
}

fn serveListenHandle(io: Io, stream: net.Stream, peer_stream: net.Stream) !void {
    defer peer_stream.close(io);
    defer stream.close(io);

    var buf_reader: [1]u8 = undefined;
    var buf_writer: [1]u8 = undefined;
    var buf_peer_reader: [1]u8 = undefined;
    var buf_peer_writer: [1]u8 = undefined;

    const reader = stream.reader(io, &buf_reader);
    const writer = stream.writer(io, &buf_writer);
    const peer_reader = peer_stream.reader(io, &buf_peer_reader);
    const peer_writer = peer_stream.writer(io, &buf_peer_writer);

    var select_buffer: [2]StreamResult = undefined;
    var select = Io.Select(StreamResult).init(io, &select_buffer);
    defer _ = select.cancel();

    select.async(.rx, pipeStream, .{ @constCast(&reader.interface), @constCast(&peer_writer.interface) });
    select.async(.tx, pipeStream, .{ @constCast(&peer_reader.interface), @constCast(&writer.interface) });

    const finish = try select.await();

    switch (finish) {
        .rx => {
            std.debug.print("client is closing\n\n", .{});
        },
        .tx => {
            std.debug.print("server is closing\n\n", .{});
        },
    }
}
