const std = @import("std");
const myzql = @import("myzql");
const Client = myzql.client.Client;
const DateTime = myzql.temporal.DateTime;
const Duration = myzql.temporal.Duration;

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    const allocator = gpa.allocator();
    defer {
        switch (gpa.deinit()) {
            .ok => {},
            .leak => std.log.err("memory leak detected", .{}),
        }
    }

    var c = Client.init(.{ .password = "password" });
    try example_ping(&c, allocator);
    try example_query(&c, allocator);
    try example_insert_query(&c, allocator);
    try example_temporal(&c, allocator);
}

fn example_ping(c: *Client, allocator: std.mem.Allocator) !void {
    try c.ping(allocator);
}

fn example_query(c: *Client, allocator: std.mem.Allocator) !void {
    {
        // Execute, expect no result
        const result = try c.query(allocator, "CREATE DATABASE testdb");
        defer result.deinit(allocator);
        const ok = try result.expect(.ok); // simplied error handling
        std.debug.print("ok: {}\n", .{ok});
    }
    {
        const result = try c.query(allocator, "DROP DATABASE testdb");
        defer result.deinit(allocator);
        switch (result.value) { // full result handling
            .ok => |ok| {
                _ = ok;
            },
            .err => |err| return err.asError(),
            .rows => |rows| {
                _ = rows;
                @panic("should not expect rows");
            },
        }
    }
    { // query returns all result in []const u8
        // below shows different way to handle result row
        const query_res = try c.query(allocator, "SELECT 8,9 UNION ALL SELECT 10,11");
        defer query_res.deinit(allocator);
        const rows = try query_res.expect(.rows);
        {
            // preallocate destination
            // good when you've already got a placeholder for result
            var dest = [_]?[]const u8{ undefined, undefined };

            const row = try rows.readRow(allocator);
            defer row.deinit(allocator);
            const data = try row.expect(.data); // simplied error handling

            try data.scan(&dest);
            std.debug.print("dest: {any}\n", .{dest});
        }
        {
            const row = try rows.readRow(allocator);
            defer row.deinit(allocator);
            const data = try row.expect(.data);

            // scanAlloc is good when you're feeling lazy
            // remember to free the result after using
            const dest = try data.scanAlloc(allocator);

            defer allocator.free(dest);
        }
        {
            const row = try rows.readRow(allocator);
            defer row.deinit(allocator);
            switch (row.value) { // full result handling
                .eof => {},
                .err => |err| return err.asError(),
                .data => @panic("unexpected data"),
            }
        }
    }

    { // iterate using while loop
        const query_res = try c.query(allocator, "SELECT 8,9 UNION ALL SELECT 10,11");
        defer query_res.deinit(allocator);
        const rows = try query_res.expect(.rows);

        const it = rows.iter();
        while (try it.next(allocator)) |row| {
            defer row.deinit(allocator);
            // do something with row
        }
    }
    { // collect all rows into a table
        const query_res = try c.query(allocator, "SELECT 8,9 UNION ALL SELECT 10,11");
        defer query_res.deinit(allocator);
        const rows = try query_res.expect(.rows);
        const it = rows.iter();
        const table = try it.collectTexts(allocator);
        defer table.deinit(allocator);

        std.debug.print("table: {any}\n", .{table.rows}); //table.rows: []const []const ?[]const u8
    }
}

fn example_insert_query(c: *Client, allocator: std.mem.Allocator) !void {
    try queryExpectOk(allocator, c, "CREATE DATABASE test");
    defer queryExpectOk(allocator, c, "DROP DATABASE test") catch {};

    try queryExpectOk(allocator, c,
        \\CREATE TABLE test.person (
        \\    name VARCHAR(255),
        \\    age INT
        \\)
    );
    defer queryExpectOk(allocator, c, "DROP TABLE test.person") catch {};

    { // Insert
        const prep_res = try c.prepare(allocator, "INSERT INTO test.person VALUES (?, ?)");
        defer prep_res.deinit(allocator);
        const prep_stmt = try prep_res.expect(.ok);
        const params = .{
            .{ "John", 42 },
            .{ "Sam", 24 },
        };
        inline for (params) |param| {
            const exe_res = try c.execute(allocator, &prep_stmt, param);
            defer exe_res.deinit(allocator);
            _ = try exe_res.expect(.ok);
        }
    }

    // Not Supported Yet
    // {
    //     const Person = struct {
    //         name: []const u8,
    //         age: u8,
    //     };
    //     const prep_res = try c.prepare(allocator, "INSERT INTO test.person VALUES (?, ?)");
    //     defer prep_res.deinit(allocator);
    //     const prep_stmt = try prep_res.expect(.ok);
    //     const params: []const Person = &.{
    //         .{ .name = "John", .age = 42 },
    //         .{ .name = "Sam", .age = 42 },
    //     };
    //     inline for (params) |param| {
    //         const exe_res = try c.execute(allocator, &prep_stmt, param);
    //         defer exe_res.deinit(allocator);
    //         _ = try exe_res.expect(.ok);
    //     }
    // }

    { // Binary Protocol Result
        const prep_res = try c.prepare(allocator, "SELECT * FROM test.person");
        defer prep_res.deinit(allocator);
        const prep_stmt = try prep_res.expect(.ok);
        const Person = struct {
            name: []const u8,
            age: u8,
        };

        { // collect all rows into a table
            const res = try c.execute(allocator, &prep_stmt, .{});
            defer res.deinit(allocator);
            const rows = try res.expect(.rows);
            const iter = rows.iter();
            const person_structs = try iter.collectStructs(Person, allocator); // convenient function for collecting all results
            defer person_structs.deinit(allocator);
            const many_people: []const Person = person_structs.rows;
            std.debug.print("many_people: {any}\n", .{many_people});
        }
        { // iterate using while loop
            const res = try c.execute(allocator, &prep_stmt, .{});
            defer res.deinit(allocator);
            const rows = try res.expect(.rows);
            const iter = rows.iter();
            while (try iter.next(allocator)) |row| {
                defer row.deinit(allocator);
                const data = try row.expect(.data);
                const person = try data.scanAlloc(Person, allocator); // there is also scan function for preallocated destination
                defer allocator.destroy(person);
                std.debug.print("person: {any}\n", .{person});
            }
        }
    }
}

// temporal type support
fn example_temporal(c: *Client, allocator: std.mem.Allocator) !void {
    try queryExpectOk(allocator, c, "CREATE DATABASE test");
    defer queryExpectOk(allocator, c, "DROP DATABASE test") catch {};

    try queryExpectOk(allocator, c,
        \\CREATE TABLE test.temporal_types_example (
        \\    event_time DATETIME(6) NOT NULL,
        \\    duration TIME(6) NOT NULL
        \\)
    );
    defer queryExpectOk(allocator, c, "DROP TABLE test.temporal_types_example") catch {};

    { // Insert
        const prep_res = try c.prepare(allocator, "INSERT INTO test.temporal_types_example VALUES (?, ?)");
        defer prep_res.deinit(allocator);
        const prep_stmt = try prep_res.expect(.ok);

        const my_time: DateTime = .{
            .year = 2023,
            .month = 11,
            .day = 30,
            .hour = 6,
            .minute = 50,
            .second = 58,
            .microsecond = 123456,
        };
        const my_duration: Duration = .{
            .days = 1,
            .hours = 23,
            .minutes = 59,
            .seconds = 59,
            .microseconds = 123456,
        };
        const params = .{
            .{ my_time, my_duration },
        };
        inline for (params) |param| {
            const exe_res = try c.execute(allocator, &prep_stmt, param);
            defer exe_res.deinit(allocator);
            _ = try exe_res.expect(.ok);
        }
    }

    { // Select
        const DateTimeDuration = struct {
            event_time: DateTime,
            duration: Duration,
        };
        const prep_res = try c.prepare(allocator, "SELECT * FROM test.temporal_types_example");
        defer prep_res.deinit(allocator);
        const prep_stmt = try prep_res.expect(.ok);
        const res = try c.execute(allocator, &prep_stmt, .{});
        defer res.deinit(allocator);
        const rows_iter = (try res.expect(.rows)).iter();

        const structs = try rows_iter.collectStructs(DateTimeDuration, allocator);
        defer structs.deinit(allocator);
        std.debug.print("structs: {any}\n", .{structs.rows}); // structs.rows: []const DateTimeDuration
        // Do something with structs
    }
}

// convenient function for testing
fn queryExpectOk(allocator: std.mem.Allocator, c: *Client, query: []const u8) !void {
    const query_res = try c.query(allocator, query);
    defer query_res.deinit(allocator);
    _ = try query_res.expect(.ok);
}
