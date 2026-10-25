const std = @import("std");
const Io = std.Io;
const net = std.Io.net;
const Allocator = std.mem.Allocator;
const Address = net.IpAddress;

const Server = @import("./Server.zig");
const net_utils = @import("./net_utils.zig");

const listen_address = "0.0.0.0";
const listen_port = 8989;

const Command = enum {
    serve,
    connect,
};

pub fn main(init: std.process.Init) !void {
    const io = init.io;
    const args = try std.process.Args.toSlice(init.minimal.args, init.arena.allocator());

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

fn runServe(io: Io, allocator: Allocator, args: ([]const [:0]const u8)) !void {
    if (args.len < 3) {
        return printServeUsage();
    }

    const from = args[2];
    const to = if (args.len >= 4) args[3] else "127.0.0.1";

    const from_addr = try net_utils.parseLocation(from);
    const to_addr = try net_utils.parseLocation(to);

    var srv = try Server.init(io, allocator, to_addr, from_addr);
    defer srv.deinit();

    try srv.listen();

    var buf: [64]u8 = undefined;

    const stdin_file = std.Io.File.stdin();
    var stdin = stdin_file.reader(io, &buf);

    std.debug.print("Listening on {} - Press ENTER to stop the server...\n", .{srv});
    _ = try stdin.interface.takeDelimiterInclusive('\n');
    std.debug.print("Stopping server...\n", .{});
}
