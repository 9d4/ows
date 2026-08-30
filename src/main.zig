const std = @import("std");
const Io = std.Io;
const net = std.Io.net;
const Allocator = std.mem.Allocator;
const Address = net.IpAddress;

const httpz = @import("httpz");
const ows = @import("ows");

const listen_address = "0.0.0.0";
const listen_port = 8989;

const Command = enum {
    serve,
    connect,
};

pub fn main(init: std.process.Init) !void {
    const io = init.io;
    const args = try std.process.Args.toSlice(init.minimal.args, init.gpa);

    if (args.len < 2) return printUsage();

    const cmd = std.meta.stringToEnum(Command, args[1]) orelse {
        std.log.err("Unkown command: {s}", .{args[1]});
        return;
    };

    switch (cmd) {
        .serve => try runServe(io, init.gpa, args),
        .connect => {},
    }
}

fn printUsage() void {
    std.debug.print("Usage: ows <command> [options]\n\n", .{});
    std.debug.print("Commands:\n", .{});
    std.debug.print("  serve         Start serving through tunnel\n", .{});
    std.debug.print("  connect       Connect to tunnel\n", .{});
}

fn printServeUsage() void {
    std.debug.print("Usage: ows serve <from> [to]\n\n", .{});
    std.debug.print("Args:\n", .{});
    std.debug.print("  from          Address to listen from (eg. 127.0.0.1:3306)\n", .{});
    std.debug.print("  to            HTTP Address to listen to (eg. 127.0.0.1:33066)\n", .{});
}

fn parseLocation(loc: []const u8) !net.IpAddress {
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

fn runServe(io: Io, allocator: Allocator, args: ([]const [:0]const u8)) !void {
    if (args.len < 3) {
        return printServeUsage();
    }

    const from = args[2];
    const to = if (args.len >= 4) args[3] else "127.0.0.1";

    const from_addr = try parseLocation(from);
    const to_addr = try parseLocation(to);

    var srv = try Server.init(io, allocator, to_addr, from_addr);
    defer srv.deinit();

    try srv.listen();

    var buf: [128]u8 = undefined;

    const stdin_file = std.Io.File.stdin();
    var stdin = stdin_file.reader(io, &buf);

    std.debug.print("Press ENTER to stop the server...\n", .{});
    _ = try stdin.interface.takeDelimiterInclusive('\n');
    std.debug.print("Stopping server...\n", .{});
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

fn pipeStream(reader: *std.Io.Reader, writer: *std.Io.Writer) void {
    _ = reader.streamRemaining(writer) catch |err| {
        std.debug.print("error streaming reader: {s}\n", .{@errorName(err)});
    };
}

const StreamResult = union(enum) {
    rx: Io.Cancelable!void,
    tx: Io.Cancelable!void,
};

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

const Server = struct {
    const Self = @This();

    server: *httpz.Server(*Handler),
    listen_thread: ?std.Thread,
    handler: *Handler,
    allocator: std.mem.Allocator,

    pub fn init(io: Io, allocator: Allocator, listen_addr: Address, peer_addr: Address) !Server {
        const server = try allocator.create(httpz.Server(*Handler));
        errdefer allocator.destroy(server);

        const handler = try Handler.init(io, allocator, peer_addr);
        errdefer allocator.destroy(handler);

        server.* = try httpz.Server(*Handler).init(io, allocator, .{
            .address = .{ .ip = listen_addr },
            .thread_pool = .{ .count = 1 },
        }, handler);

        var router = try server.router(.{});
        router.get("/ping", Handler.ping, .{});

        return .{
            .allocator = allocator,
            .server = server,
            .listen_thread = null,
            .handler = handler,
        };
    }

    pub fn deinit(self: *Self) void {
        self.server.stop();
        defer self.server.deinit();
        defer self.allocator.destroy(self.server);
        defer self.handler.deinit(self.allocator);

        if (self.listen_thread) |thread| {
            thread.join();
            thread.detach();
        }
    }

    pub fn listen(self: *Self) !void {
        var srv = self.server;
        const thread = try srv.listenInNewThread();
        self.listen_thread = thread;
    }
};

const Handler = struct {
    io: Io,
    tcp_peer_address: Address,

    pub fn init(io: Io, allocator: Allocator, tcp_peer_address: Address) !*Handler {
        const self = try allocator.create(Handler);
        errdefer allocator.destroy(self);

        self.io = io;
        self.tcp_peer_address = tcp_peer_address;

        return self;
    }

    pub fn deinit(self: *Handler, allocator: Allocator) void {
        allocator.destroy(self);
    }

    pub fn notFound(_: *Handler, req: *httpz.Request, res: *httpz.Response) !void {
        const upgraded = try httpz.upgradeWebsocket(WebsocketHandler, req, res, WebsocketContext{});
        if (!upgraded) {
            res.status = 400;
            res.body = "Invalid websocket handshake";
            return;
        }
    }

    pub fn ping(_: *Handler, _: *httpz.Request, res: *httpz.Response) !void {
        res.body = "pong";
    }

    const WebsocketContext = struct {};

    pub const WebsocketHandler = struct {
        conn: *httpz.websocket.Conn,

        pub fn init(conn: *httpz.websocket.Conn, _: WebsocketContext) !WebsocketHandler {
            return .{ .conn = conn };
        }

        pub fn clientMessage(self: *WebsocketHandler, data: []const u8) !void {
            try self.conn.write(data);
        }
    };

    pub fn connectPeer(self: *Handler) !void {
        _ = self.tcp_peer_address.connect(self.io, .{
            .mode = .stream,
            .protocol = .tcp,
        }) catch |err| {
            std.debug.print("connection to target failed: {f}: {s}\n\n", .{ self.tcp_peer_address, @errorName(err) });

            // var w = stream.writer(io, &.{});
            // _ = w.interface.write("connection to target failed") catch {};
            // _ = w.interface.flush() catch {};
            // stream.close(io);

            // continue;
        };
    }
};
