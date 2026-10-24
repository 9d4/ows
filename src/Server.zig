const Self = @This();

const std = @import("std");
const Io = std.Io;
const Allocator = std.mem.Allocator;
const net = std.Io.net;
const Address = net.IpAddress;
const httpz = @import("httpz");
const net_utils = @import("net_utils.zig");
const Tunnel = @import("Tunnel.zig");

server: *httpz.Server(*Handler),
listen_thread: ?std.Thread,
handler: *Handler,
allocator: std.mem.Allocator,

pub fn init(io: Io, allocator: Allocator, listen_addr: Address, peer_addr: Address) !Self {
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

pub const Handler = struct {
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

    pub fn notFound(self: *Handler, req: *httpz.Request, res: *httpz.Response) !void {
        const ctx = WebsocketContext{
            .io = self.io,
            .num = 200,
            .peer_address = self.tcp_peer_address,
        };
        const upgraded = try httpz.upgradeWebsocket(WebsocketHandler, req, res, ctx);
        if (!upgraded) {
            res.status = 400;
            res.body = "Invalid websocket handshake";
            return;
        }
    }

    pub fn ping(_: *Handler, _: *httpz.Request, res: *httpz.Response) !void {
        res.body = "pong";
    }

    const WebsocketContext = struct {
        io: Io,
        num: u8 = 0,
        peer_address: Address,
    };

    pub const WebsocketHandler = struct {
        conn: *httpz.websocket.Conn,
        ctx: WebsocketContext,
        counter: u32 = 0,
        peer_stream: net.Stream = undefined,
        peer_writer: net.Stream.Writer = undefined,

        pub fn init(conn: *httpz.websocket.Conn, ctx: WebsocketContext) !WebsocketHandler {
            std.debug.print("Handler Init is called; ctx num: {d}\n", .{ctx.num});

            const stream = try net_utils.connectAddress(ctx.io, ctx.peer_address);
            const writer = stream.writer(ctx.io, &.{});

            const self = WebsocketHandler{
                .conn = conn,
                .ctx = ctx,
                .peer_stream = stream,
                .peer_writer = writer,
            };

            return self;
        }

        pub fn afterInit(self: *WebsocketHandler, _: WebsocketContext) !void {
            _ = self.ctx.io.async(Tunnel.pipeNetStreamToWS, .{
                self.ctx.io,
                self.peer_stream,
                self.conn,
            });
        }

        pub fn clientMessage(self: *WebsocketHandler, data: []const u8) !void {
            self.ctx.num += 1;
            self.counter += 1;

            std.debug.print("Message counter: {d}\n", .{self.counter});
            _ = try self.peer_writer.interface.write(data);
        }
    };
};
