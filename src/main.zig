const std = @import("std");
const myzql = @import("myzql");
const Conn = myzql.conn.Conn;
const DateTime = myzql.temporal.DateTime;
const Duration = myzql.temporal.Duration;
const OkPacket = myzql.protocol.generic_response.OkPacket;
const ResultSet = myzql.result.ResultSet;
const TextResultRow = myzql.result.TextResultRow;
const ResultRowIter = myzql.result.ResultRowIter;
const TextElemIter = myzql.result.TextElemIter;
const TextElems = myzql.result.TextElems;
const PreparedStatement = myzql.result.PreparedStatement;
const BinaryResultRow = myzql.result.BinaryResultRow;

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    const allocator = gpa.allocator();
    defer {
        switch (gpa.deinit()) {
            .ok => {},
            .leak => std.log.err("memory leak detected", .{}),
        }
    }

    var c = try Conn.init(allocator, &.{ .password = "password" });
    defer c.deinit();
    try c.ping();

    try exampleQuery(&c, allocator);
    try syntaxError(&c, allocator);
    try exampleSelectTextProtocol(&c, allocator);
    try exampleBinaryProtocol(&c, allocator);
    try exampleTemporal(&c, allocator);
}

// query database create and drop
fn exampleQuery(c: *Conn, allocator: std.mem.Allocator) !void {
    {
        const qr = try c.query(allocator, "CREATE DATABASE testdb");
        _ = try qr.expect(.ok);
    }
    {
        const qr = try c.query(allocator, "DROP DATABASE testdb");
        defer qr.deinit(allocator);
        _ = try qr.expect(.ok);
    }
}

fn syntaxError(c: *Conn, allocator: std.mem.Allocator) !void {
    const qr = try c.query(allocator, "garbage query");
    defer qr.deinit(allocator);
    switch (qr) {
        .err => |e| std.log.err("this error is expected: {s}", .{e.error_message}),
        else => std.log.err("expect error but got: {any}", .{qr}),
    }
}

fn exampleSelectTextProtocol(c: *Conn, allocator: std.mem.Allocator) !void {
    { // Iterating over rows and elements
        const query_res = try c.query(allocator,
            \\ SELECT 1, "2", 3.14, "4"
            \\ UNION ALL
            \\ SELECT 5, "6", null, "8"
        );
        defer query_res.deinit(allocator);

        const rows: ResultSet(TextResultRow) = try query_res.expect(.rows);
        const rows_iter: ResultRowIter(TextResultRow) = rows.iter();
        while (try rows_iter.next()) |row| { // ResultRow(TextResultRow)
            var elems_iter: TextElemIter = row.iter();
            while (elems_iter.next()) |elem| { // ?[] const u8
                std.debug.print("{?s} ", .{elem});
            }
        }
    }
    { // Iterating over rows, collecting elements into []const ?[]const u8
        const query_res = try c.query(allocator, "SELECT 3, 4, null, 6, 7");
        defer query_res.deinit(allocator);

        const rows: ResultSet(TextResultRow) = try query_res.expect(.rows);
        const rows_iter: ResultRowIter(TextResultRow) = rows.iter();
        while (try rows_iter.next()) |row| {
            const elems: TextElems = try row.textElems(allocator);
            defer elems.deinit(allocator); // elems are valid until deinit is called
            std.debug.print("elems: {any}\n", .{elems.elems});
        }
    }
    { // Collecting all elements into a table
        const query_res = try c.query(allocator,
            \\SELECT 8,9
            \\UNION ALL
            \\SELECT 10,11
        );
        defer query_res.deinit(allocator);

        const rows: ResultSet(TextResultRow) = try query_res.expect(.rows);
        const table = try rows.tableTexts(allocator);
        defer table.deinit(allocator); // table is valid until deinit is called
        std.debug.print("table: {any}\n", .{table.table});
    }
}

fn exampleBinaryProtocol(c: *Conn, allocator: std.mem.Allocator) !void {
    // Database and table setup
    try queryExpectOk(allocator, c, "CREATE DATABASE testdb");
    defer queryExpectOkOrLog(allocator, c, "DROP DATABASE testdb");
    try queryExpectOk(allocator, c,
        \\CREATE TABLE testdb.person (
        \\  id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
        \\  name VARCHAR(255),
        \\  age INT
        \\)
    );
    defer queryExpectOkOrLog(allocator, c, "DROP TABLE testdb.person");

    { // Insert
        const prep_res = try c.prepare(allocator, "INSERT INTO testdb.person (name, age) VALUES (?, ?)");
        defer prep_res.deinit(allocator);
        const prep_stmt: PreparedStatement = try prep_res.expect(.stmt);
        const params = .{
            .{ "John", 42 },
            .{ "Sam", 24 },
        };

        inline for (params) |param| {
            const exe_res = try c.execute(allocator, &prep_stmt, param);
            defer exe_res.deinit(allocator);
            const ok: OkPacket = try exe_res.expect(.ok); // expecting ok here because there's no rows returned
            const last_insert_id: u64 = ok.last_insert_id;
            std.debug.print("last_insert_id: {any}\n", .{last_insert_id});
        }
    }

    const Person = struct {
        name: []const u8,
        age: u8,

        fn greet(self: @This()) void {
            std.debug.print("Hello, {s}! You are {d} years old.\n", .{ self.name, self.age });
        }
    };

    { // Select
        const query =
            \\SELECT name, age
            \\FROM testdb.person
        ;
        const prep_res = try c.prepare(allocator, query);
        defer prep_res.deinit(allocator);
        const prep_stmt: PreparedStatement = try prep_res.expect(.stmt);

        { // Iterating over rows, scanning into struct or creating struct
            const query_res = try c.execute(allocator, &prep_stmt, .{}); // no parameters because there's no ? in the query
            defer query_res.deinit(allocator);
            const rows: ResultSet(BinaryResultRow) = try query_res.expect(.rows);
            const rows_iter = rows.iter();
            while (try rows_iter.next()) |row| {
                { // scanning into preallocated person
                    var person: Person = undefined;
                    try row.scan(&person);
                    person.greet();
                    // Important: if any field is a string, it will be valid until the next row is scanned
                    // or next query. If your rows return have strings and you want to keep the data longer,
                    // use the method below instead.
                }
                { // passing in allocator to create person
                    const person_ptr = try row.structCreate(Person, allocator);

                    // Important: please use BinaryResultRow.structDestroy
                    // to destroy the struct created by BinaryResultRow.structCreate
                    // if your struct contains strings.
                    // person is valid until BinaryResultRow.structDestroy is called.
                    defer BinaryResultRow.structDestroy(person_ptr, allocator);
                    person_ptr.greet();
                }
            }
        }
        { // collect all rows into a table ([]const Person)
            const query_res = try c.execute(allocator, &prep_stmt, .{}); // no parameters because there's no ? in the query
            defer query_res.deinit(allocator);
            const rows: ResultSet(BinaryResultRow) = try query_res.expect(.rows);
            const rows_iter = rows.iter();
            const person_structs = try rows_iter.tableStructs(Person, allocator);
            defer person_structs.deinit(allocator); // data is valid until deinit is called
            std.debug.print("person_structs: {any}\n", .{person_structs.struct_list.items});
        }
    }
}

// Date and time types
fn exampleTemporal(c: *Conn, allocator: std.mem.Allocator) !void {
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
        const prep_stmt: PreparedStatement = try prep_res.expect(.stmt);

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
        const params = .{.{ my_time, my_duration }};
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
        const prep_stmt: PreparedStatement = try prep_res.expect(.stmt);
        const res = try c.execute(allocator, &prep_stmt, .{});
        defer res.deinit(allocator);
        const rows: ResultSet(BinaryResultRow) = try res.expect(.rows);
        const rows_iter = rows.iter();

        const structs = try rows_iter.tableStructs(DateTimeDuration, allocator);
        defer structs.deinit(allocator);
        std.debug.print("structs: {any}\n", .{structs.struct_list.items}); // structs.rows: []const DateTimeDuration
        // Do something with structs
    }
}

// convenient function for testing
fn queryExpectOk(allocator: std.mem.Allocator, c: *Conn, query: []const u8) !void {
    const query_res = try c.query(allocator, query);
    defer query_res.deinit(allocator);
    _ = try query_res.expect(.ok);
}

fn queryExpectOkOrLog(allocator: std.mem.Allocator, c: *Conn, query: []const u8) void {
    queryExpectOk(allocator, c, query) catch |err| {
        std.log.err("error: {any}", .{err});
    };
}
